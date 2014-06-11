package main

import (
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"path"
	"runtime"
	"sync"
	"bufio"
	"strings"
)

const (
	fsizeSmallThreshold int64 = 1048576 // 1MB
	fsizeMediumThreshold int64 = 21943040 // 40MB
)

var numSmallFiles int64
var numMediumFiles int64
var numLargeFiles int64
var maxHashBytes int
var fastFingerprint int
var debug int8

type FileObj struct {
	path string
	size int64
	firstPassHash string
}
var fileList map[string]string
var dupFileList map[string]string

type FileObjList []FileObj
var smallFiles FileObjList
var mediumFiles FileObjList
var largeFiles FileObjList

var validFileTypes = map[string]int {
		".3gp": 1,
		".mp4": 1,
		".flv": 1,
		".mpeg": 1,
		".mkv": 1,
		".gif": 1,
		".png": 1,
		".jpg": 1,
		".jpeg": 1,
		".bmp": 1,
		".txt": 1,
		".log": 1,
		".iso": 1,
		".deb": 1,
		".wmv": 1,
		".html": 1,
		".css": 1,
		".js": 1,
	}



func main() {


	runtime.GOMAXPROCS(runtime.NumCPU())


	debug = 1
	
	// First, scan all the files and add them to their designated list
	var fileDir, outFile string

	flag.StringVar(&fileDir, "d", ".", "Directory to scan for files")
	flag.IntVar(&maxHashBytes, "b", 4096, "Max bytes to hash (4096 = default, 0 = whole file)")
	flag.IntVar(&fastFingerprint, "f", 1, "Use fast-fingerprint mode (default = true)")
	flag.StringVar(&outFile, "o", "", "File to dump report to")
	flag.Parse()


	if fastFingerprint == 0 {
		maxHashBytes = 0
	}

	fmt.Println("\nScaning all files in: ", fileDir)

	err := filepath.Walk(fileDir, scanFile)
	if err != nil {
		fmt.Printf("filepath.Walk() returned %v\n", err)
	}


	totalFiles := (len(smallFiles) + len(mediumFiles) + len(largeFiles))
	// Exit immediately if no files were found
	if totalFiles == 0 {
		fmt.Println("No files found!\n")
		os.Exit(0)
	}

	// Create the buffered channels that holds the files
	jobSmallFiles := make(chan FileObj, len(smallFiles))
	jobMediumFiles := make(chan FileObj, len(mediumFiles))
	jobLargeFiles := make(chan FileObj, len(largeFiles))
	

	// Create the waitgroup for the three file size types	
	var wg2 sync.WaitGroup
	wg2.Add(3)

	fileListSmall := map[string]string{}
	dupFileListSmall := map[string]string{}	
	fileListMedium := map[string]string{}
	dupFileListMedium := map[string]string{}
	fileListLarge := map[string]string{}
	dupFileListLarge := map[string]string{}	


	fmt.Println("")
	// Push the files into their respective channels to get processed
	fmt.Println("\tAdding small files to queue...")
	//fmt.Println("\t\tList size:", len(smallFiles))
	for _,obj := range smallFiles {
		jobSmallFiles <- obj
	}

	fmt.Println("\tAdding medium files to queue...")
	//fmt.Println("\t\tList size:", len(mediumFiles))
	for _,obj := range mediumFiles {
		if debug >= 2 { fmt.Println("DEBUG:", obj) }
		jobMediumFiles <- obj
	}

	fmt.Println("\tAdding large files to queue...")
	//fmt.Println("\t\tList size:", len(largeFiles))
	for _,obj := range largeFiles {
		if debug >= 2 { fmt.Println("DEBUG:", obj) }
		jobLargeFiles <- obj
	}
	fmt.Println("")

	// Now start three seperate threads to process the file queues (small, medium, and large)
	go func(wg2 *sync.WaitGroup, jobSmallFiles chan FileObj, fileListSmall *map[string]string, dupFileListSmall *map[string]string){
		fmt.Println("\tProcessing small file group...")
		processFileGroup(jobSmallFiles, fileListSmall, dupFileListSmall)	
		wg2.Done()
	}(&wg2, jobSmallFiles, &fileListSmall, &dupFileListSmall)

	go func(wg2 *sync.WaitGroup, jobMediumFiles chan FileObj, fileListMedium *map[string]string, dupFileListMedium *map[string]string){
		fmt.Println("\tProcessing medium file group...")
		processFileGroup(jobMediumFiles, fileListMedium, dupFileListMedium)
		wg2.Done()
	}(&wg2, jobMediumFiles, &fileListMedium, &dupFileListMedium)

	go func(wg2 *sync.WaitGroup, jobLargeFiles chan FileObj, fileListLarge *map[string]string, dupFileListLarge *map[string]string){
		fmt.Println("\tProcessing large file group...")
		processFileGroup(jobLargeFiles, fileListLarge, dupFileListLarge)
		wg2.Done()
	}(&wg2, jobLargeFiles, &fileListLarge, &dupFileListLarge)

	wg2.Wait()

	fmt.Println("")
	fmt.Printf("%42s\n", strings.Repeat("-", 43)) 
	fmt.Printf("|%21s%20s|\n", "Results", "") 
	fmt.Printf("%42s\n", strings.Repeat("-", 43)) 
	fmt.Printf("|%20s|%20d|\n", "Total Files ", totalFiles)
	fmt.Printf("|%20s|%20d|\n", "Total Dups ", (len(dupFileListSmall)+len(dupFileListMedium)+len(dupFileListLarge)))


	// If an output file hasn't been specified, then use the default name
	if outFile == "" {
		outFile = "dup-results.txt"
	}
	f,_ := os.Create(outFile)
	defer f.Close()
	w := bufio.NewWriter(f)


	// Now buffer all the data to the bufio writer
	for k,v := range fileListSmall {
		if _,ok := dupFileListSmall[k]; ok {
			_,_ = w.WriteString(fmt.Sprintf("Hash: %s\n\toriginal: %s\n\tdup: %s\n\n", k, v, dupFileListSmall[k]))
		}
	}
	for k,v := range fileListMedium {
		if _,ok := dupFileListMedium[k]; ok {
			_,_ = w.WriteString(fmt.Sprintf("Hash: %s\n\toriginal: %s\n\tdup: %s\n\n", k, v, dupFileListMedium[k]))
		}
	}
	for k,v := range fileListLarge {
		if _,ok := dupFileListLarge[k]; ok {
			_,_ = w.WriteString(fmt.Sprintf("Hash: %s\n\toriginal: %s\n\tdup: %s\n\n", k, v, dupFileListLarge[k]))
		}
	}

	// Flush the buffer to the file
	w.Flush()
	
	fmt.Printf("|%20s|%20s|\n", "Report File ", outFile)
	fmt.Println("\n")

}


func processFileGroup(fileGroup chan FileObj, fileList *map[string]string, dupFileList *map[string]string) {


	filesInGroup := len(fileGroup)
	for i := 0; i < filesInGroup; i++ {		
		f := <-fileGroup
		useFastFp := false 

		// Add some aditional output if debug mode at least 2
		if debug >= 2 {
			if f.size < fsizeSmallThreshold {
				fmt.Println("\t*Small File:", f.path)
			} else if f.size >= fsizeSmallThreshold && f.size < fsizeMediumThreshold{
				fmt.Println("\t*Medium File:", f.path)
			} else {
				fmt.Println("\t*Large File:", f.path)
			}
		} 

		// If the file is greater than 1MB, use the fast-finger print approach
		if f.size > fsizeSmallThreshold && fastFingerprint == 1 {
			useFastFp = true
		} 

		fl := *fileList
		dl := *dupFileList
		md5h,_ := getFileHash(f.path, maxHashBytes, useFastFp)			
		md5Hash := hex.EncodeToString(md5h)

		if _,ok := fl[md5Hash]; !ok {
			fl[md5Hash] = f.path
		} else {
			dl[md5Hash] = f.path
		}


		if debug >= 2 {
			fmt.Println("\t\tHash:", md5Hash)
		}

	}	
}


func scanFile(fpath string, f os.FileInfo, err error) error {

	fname := filepath.Base(fpath)
	dir, err := filepath.Abs(filepath.Dir(fpath))
	if err != nil {
		fmt.Println("\t", err)
		return err
	}

	// If it's not a file, then return immediately
	file, err := os.Open(fpath)
	defer file.Close()
	if err != nil {
		if debug >= 2 {
			fmt.Println("Error opening file:", err)
		}
		return nil
	}

	finfo, err := file.Stat()
	if err != nil {
		if debug >= 2 {
			fmt.Println("Error getting file stats:", err)
		}
		return nil
	}

	mode := finfo.Mode()

	if err != nil {
		return nil
	}

	// Ensure that it's a valid file type
	ext := path.Ext(fname)
	_,ok := validFileTypes[ext]

	// Ensure that it's not a directory
	if mode.IsDir() {
		return nil
	} else if ok {
		size := finfo.Size()
		/*
			Small channel: filesize < 1MB (1048576 bytes)
			Medium channel:  1MB <= filesize <= 40MB (41943040 bytes)
			Large channel: filesize > 40MB (41943041 bytes)
		*/

		if size < fsizeSmallThreshold {
			//if debug >= 1 { fmt.Println("\t***** Adding File to small list:", fname) }
			smallFiles = append(smallFiles, FileObj{path: dir+"/"+fname, size: size})
		} else if size >= fsizeSmallThreshold && size < fsizeMediumThreshold{
			//if debug >= 1 { fmt.Println("\t***** Adding File to medium list:", fname) }
			mediumFiles = append(mediumFiles, FileObj{path: dir+"/"+fname, size: size})
		} else {
			//if debug >= 1 { fmt.Println("\t***** Adding File to large list:", fname) }
			largeFiles = append(largeFiles, FileObj{path: dir+"/"+fname, size: size})
		}
		

	} // End out else

	return nil
}


func getFileHash(filePath string, maxBytes int, fastFingerPrint bool) ([]byte, int64) {

	fi, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()

        // Get the filesize for the fstat system call
        fstat,err := os.Stat(filePath)
        if err != nil {
           panic(err)
        }
        fSize := fstat.Size()

	// Used to be 1K, but increased to 4K in order to decrease number of reads
	var buf []byte
        buffSize1 := 2048
        buffSize2 := 4096

       
        if fastFingerPrint == true {
        
           if fSize < int64(buffSize1) {
              
             buf = make([]byte, fSize)
             buffSize1 = int(fSize/3)             
      
           } else {     
             buf = make([]byte, buffSize1)
           }

        } else {

          if fSize < int64(buffSize2) {

             buf = make([]byte, fSize)
             buffSize2 = int(fSize/3)
          } else {
             buf = make([]byte, buffSize2)
          }

        }

	hash := sha1.New()

        // If fast finger print option is enable, the determine the 3 parts where a finger print will be taken
        fpBlock := make([]int64, 3)
        if fastFingerPrint == true {
            fpBlock[0] = 0
            fpBlock[1] = ( fSize/int64(2) ) - ( int64(buffSize1)/int64(2) )
            fpBlock[2] = fSize - int64(buffSize1) 
        }

	var totalBytes int64

	// Now loop and fill the buffer with data until non is left
        i := 0
        n := 0
	for {
                
                if fastFingerPrint == true {
		   n, err = fi.ReadAt(buf, fpBlock[i])
                   i++
                } else {
                  n, err = fi.Read(buf)
                }

		if err != nil && err != io.EOF {
	               fmt.Println()
        		panic(err)
		}

		totalBytes += int64(n)
		
                // If no more data to enter into the buffer, then break out of loop
		if n == 0 {
			break
		}

                // if it's a fast finger print, and all 3 pieces have been read, then break out
                if fastFingerPrint == true && i >= 2 {
                       break
                }

		if _, err := io.WriteString(hash, string(buf[:n])); err != nil {
			panic(err)
		}

		// If we only want to read a certain ammount of bytes, then return when we reach that number
		if maxBytes > 0 && totalBytes >= int64(maxBytes) || totalBytes > fSize {
			break
		}
	}
	

	return hash.Sum(nil), totalBytes

}
