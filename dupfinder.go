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
)


var numSmallFiles int64
var numMediumFiles int64
var numLargeFiles int64
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
	}



func main() {


	// Should try convert the app to be more similar to this model
	// http://golangtutorials.blogspot.ca/2011/06/channels-in-go-range-and-select.html
	// As a file is scanned, it should be pushed on the chanel be hashed

	if 2 > runtime.NumCPU() {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(2)
	}

	debug = 0
	
	// First, scan all the files and add them to their designated list
	var fileDir, outFile string
        var fastFp bool

	flag.StringVar(&fileDir, "d", ".", "Base directory where to start the recursive search for video files")
        flag.BoolVar(&fastFp, "f", false, "Enable fast-finger printing")
	flag.StringVar(&outFile, "o", "", "File to dump report to")

	flag.Parse()

	fmt.Println("\nScaning all files in: ", fileDir)

	err := filepath.Walk(fileDir, scanFile)
	if err != nil {
		fmt.Printf("filepath.Walk() returned %v\n", err)
	}

	jobSmallFiles := make(chan FileObj, len(smallFiles))
	jobMediumFiles := make(chan FileObj, len(mediumFiles))
	jobLargeFiles := make(chan FileObj, len(largeFiles))
	totalFiles := (len(smallFiles) + len(mediumFiles) + len(largeFiles))
	
	var wg sync.WaitGroup
	wg.Add(3)
	
	fmt.Println("Pushing small files to queue...")
	for _,obj := range smallFiles {
		jobSmallFiles <- obj
	}

	fmt.Println("Pushing medium files to queue...")
	for _,obj := range mediumFiles {
		jobMediumFiles <- obj
	}

	fmt.Println("Pushing large files to queue...")
	for _,obj := range largeFiles {
		jobLargeFiles <- obj
	}



	fileListSmall := make(map[string]string, totalFiles)
	dupFileListSmall := map[string]string{}

	fileListMedium := make(map[string]string, totalFiles)
	dupFileListMedium := map[string]string{}

	fileListLarge := make(map[string]string, totalFiles)
	dupFileListLarge := map[string]string{}

	// Now start three seperate threads to process the file queues (small, medium, and large)	
	go processFileGroup(&wg, jobSmallFiles, &fileListSmall, &dupFileListSmall)
	go processFileGroup(&wg, jobMediumFiles, &fileListMedium, &dupFileListMedium)
	go processFileGroup(&wg, jobLargeFiles, &fileListLarge, &dupFileListLarge)

	wg.Wait()
		
	close(jobSmallFiles)
	close(jobMediumFiles)
	close(jobLargeFiles)

	fmt.Println("Results:") 
	fmt.Println("\tTotal Files:", (len(fileListSmall)+len(fileListMedium)+len(fileListLarge))) 
	fmt.Println("\tTotal Dups:", (len(dupFileListSmall)+len(dupFileListMedium)+len(dupFileListLarge))) 


	// Now write all the results to a file
	if outFile == "" {
		outFile = "dup-results.txt"
	}
	f,_ := os.Create(outFile)
	defer f.Close()
	w := bufio.NewWriter(f)

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
	

}


func processFileGroup(wg *sync.WaitGroup, fileGroup chan FileObj, fileList *map[string]string, dupFileList *map[string]string) {
	for i := 0; i < len(fileGroup); i++ {		
		f := <-fileGroup
		useFastFp := false 

		if debug >= 2 {
			if f.size < 1048576 {
				fmt.Println("\t*Small File:", f.path)
			} else if f.size >= 1048576 && f.size < 21943040{
				fmt.Println("\t*Medium File:", f.path)
			} else {
				fmt.Println("\t*Large File:", f.path)
			}
		}

		// If the file is less than 1MB, use the fast-finger print approach
		if f.size < 1048576 {
			useFastFp = true
		} 

		fl := *fileList
		dl := *dupFileList
		md5h,_ := getFileHash(f.path, 4096, useFastFp)			
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
	wg.Done()
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

		if size < 1048576 {
			smallFiles = append(smallFiles, FileObj{path: dir+"/"+fname, size: size})
		} else if size >= 1048576 && size < 21943040{
			mediumFiles = append(mediumFiles, FileObj{path: dir+"/"+fname, size: size})
		} else {
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
