#GoDedup - A Go Duplicate File Detector
=======================================
Usage: 

go run dupfinder.go -d [DIR_PATH] 

or 

./dupfinder -d [DIR_PATH]


Optional Arguments:

-o [OUTPUT_FILE] : The output file to which the report will be written to. Default is dup-results.txt
-b [READ_MAX_BYTES] : The max number of bytes to read from a file in order to create the SHA1 hash.  Default is 4096, 0 means read the whole file.