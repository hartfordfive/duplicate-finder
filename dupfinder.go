package main

import (
	//"bufio"
	//"crypto/sha1"
	//"encoding/hex"
	"flag"
	"fmt"
	//"github.com/boltdb/bolt"
	//"io"
	//"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	//"strings"
	//"encoding/json"
	"github.com/HouzuoGuo/tiedot/db"
	_ "github.com/HouzuoGuo/tiedot/dberr"
	"sync"
	//"time"
	"github.com/steakknife/bloomfilter"
)

const (
	fsizeSmallThreshold int64 = 1048576 // 1MB
	maxElements               = 200000
	probCollide               = 0.0000001
)

var maxHashBytes int
var fastFingerprint int
var debug int8

//var dbConn *db.DB

//var collection_files *db.Col

var dupFileList chan File
var filesToProcess chan File
var doneScanning chan bool
var bf *bloomfilter.Filter

var validFileTypes = map[string]int{
	".3gp":  1,
	".mp4":  1,
	".flv":  1,
	".mpeg": 1,
	".mkv":  1,
	".gif":  1,
	".png":  1,
	".jpg":  1,
	".jpeg": 1,
	".bmp":  1,
	".txt":  1,
	".log":  1,
	".iso":  1,
	".deb":  1,
	".wmv":  1,
	".html": 1,
	".css":  1,
	".js":   1,
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

	filesToProcess = make(chan File, 10000)
	dupFileList = make(chan File, 10000)
	doneScanning = make(chan bool)

	fmt.Println("\nScaning all files in: ", fileDir)

	bf = bloomfilter.NewOptimal(maxElements, probCollide)

	// Start a go routine to gather the list of files
	go func() {
		filepath.Walk(fileDir, func(fpath string, f os.FileInfo, _ error) (err error) {
			//chann <- path
			//return
			fname := filepath.Base(fpath)

			//fmt.Println("Basename:", fname)

			dir, err := filepath.Abs(filepath.Dir(fpath))
			if err != nil {
				fmt.Println("\t", err)
				return
			}

			// If it's not a file, then return immediately
			file, err := os.Open(fpath)
			defer file.Close()
			if err != nil {
				if debug >= 2 {
					fmt.Println("Error opening file:", err)
				}
				return
			}

			finfo, err := file.Stat()
			if err != nil {
				if debug >= 2 {
					fmt.Println("Error getting file stats:", err)
				}
				return
			}

			mode := finfo.Mode()

			if err != nil {
				return
			}

			// Ensure that it's a valid file type
			ext := path.Ext(fname)
			_, ok := validFileTypes[ext]

			// Ensure that it's not a directory
			if mode.IsDir() {
				return
			} else if ok {
				size := finfo.Size()
				// Now simply add the file on the toProcess channel
				filesToProcess <- File{Path: dir + "/" + fname, Size: size, FastFingerprint: false}
			}

			return
		})
		defer close(filesToProcess)
	}()

	// ---------- OPEN AND PREP THE DB -----------
	fileDB := "dff.db"
	os.RemoveAll(fileDB)
	defer os.RemoveAll(fileDB)

	// (Create if not exist) open a database
	dbConn, err := db.OpenDB(fileDB)
	if err != nil {
		panic(err)
	}

	// Create the files collection
	if err := dbConn.Create("Files"); err != nil {
		panic(err)
	}
	if err = dbConn.Create("DuplicateFiles"); err != nil {
		panic(err)
	}

	// Create indices
	col_files := dbConn.Use("Files")
	if err := col_files.Index([]string{"Path", "Hash"}); err != nil {
		panic(err)
	}
	if err := col_files.Index([]string{"Path"}); err != nil {
		panic(err)
	}
	if err := col_files.Index([]string{"Hash"}); err != nil {
		panic(err)
	}

	col_dup_files := dbConn.Use("Files")
	if err := col_dup_files.Index([]string{"Path", "Hash"}); err != nil {
		panic(err)
	}
	if err := col_dup_files.Index([]string{"Path"}); err != nil {
		panic(err)
	}
	if err := col_dup_files.Index([]string{"Hash"}); err != nil {
		panic(err)
	}

	// Start another goroutine to generate the hashes of the files and place
	// them on another channel

	var wg sync.WaitGroup
	wg.Add(1)

	fmt.Println("\tRunning hash generator...")
	go processFiles(&wg, filesToProcess, dupFileList, doneScanning, dbConn)
	wg.Wait()

	// Once all the file hashes are generated, go over the list of  files/hashes and build
	// the duplicate list

}

/*
func getDuplicateList() {

	var query interface{}

	json.Unmarshal([]byte(`[{"eq": "[HASH]", "in": ["Hash"]}]`), &query)

	queryResult := make(map[int]struct{}) // query result (document IDs) goes into map keys

	if err := db.EvalQuery(query, feeds, &queryResult); err != nil {
		panic(err)
	}

	for id := range queryResult {
		readBack, err := feeds.Read(id)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Query returned document %v\n", readBack)
	}
}
*/

func processFiles(wg *sync.WaitGroup, filesToProcess chan File, dupFileList chan File, doneScanning chan bool, dbConn *db.DB) {

	col_files := dbConn.Use("Files")
	col_dup_files := dbConn.Use("DuplicateFiles")

	for f := range filesToProcess {

		//select {
		//case f := <-filesToProcess:

		// If the file is greater than 1MB, use the fast-finger print approach
		if f.Size > fsizeSmallThreshold && fastFingerprint == 1 {
			f.FastFingerprint = true
		}

		md5h, _, err := f.GetFileHash(maxHashBytes)
		f.Hash = md5h

		fmt.Println("File:", f.Path)
		fmt.Println("\tHash:", f.Hash)
		fmt.Println("\tError:", err)

		// time.Now().Format(time.RFC3339) // Date added

		// If the hash isn't already in the bloom filter, then add it
		if !bf.Contains(f.Hash) { // probably true, could be false

			bf.Add(f.Hash)
			_, err = col_files.Insert(map[string]interface{}{
				"Path":            f.Path,
				"Size":            f.Size,
				"Hash":            f.Hash,
				"FastFingerprint": f.FastFingerprint})

		} else { // Else a duplicate has hit

			// -------- 1. Find the original file with the given hash ----------
			var query interface{}
			json_query := "{\"eq\": \"" + f.Hash + "\", \"in\": [\"Hash\"], \"limit\": 1}"
			// []byte(`[{"eq": "[HASH]", "in": ["Hash"]}]`)

			json.Unmarshal([]byte(json_query), &query)
			queryResult := make(map[int]struct{}) // query result (document IDs) goes into map keys

			if err := db.EvalQuery(query, col_files, &queryResult); err != nil {
				panic(err)
			}

			var duplicates []string
			for id := range queryResult {

				readBack, err := col_files.Read(id)
				if err != nil {
					panic(err)
				}
				fmt.Printf("Query returned document %v\n", readBack)

				if _, ok := readBack["DuplicateCopies"]; ok {
					// Other previously found duplicates, so add this one to the list
					duplicates = []string{f.Path}
					for elem := range readBack["DuplicateCopies"].([]string) {
						// Now add all the previous duplicates to the updatd list
						duplicates = append(duplicates, elem)
					}
					// Finally update the record
					err = col_files.Update(id, map[string]interface{}{
						"Path":            readBack["Path"],
						"Size":            readBack["Size"],
						"Hash":            readBack["Hash"],
						"FastFingerprint": readBack["FastFingerprint"],
						"Duplicates":      duplicates,
					})
					if err != nil {
						panic(err)
					}
				} else {
					// No duplicates logged so far, so add this one as a first
					// ---------- 3. Update the document with the new duplicate found   ---------
				}
			}

			// ---------- 2. Update the document with the new duplicate found   ---------

		/*
			case _ = <-doneScanning:
				if err := dbConn.Close(); err != nil {
					panic(err)
				}
				wg.Done()
				return
				//continue CompleteLoop
			}
		*/
	}

	wg.Done()
	return

	//CompleteLoop:

}
