#DupFinder
=======================================

A simple duplicate file finder built in Go.


##Usage: 

`go run dupfinder.go -d [DIR_PATH]`

or 

`./dupfinder -d [DIR_PATH]`


**Optional Arguments**:

`-o [OUTPUT_FILE]` : The output file to which the report will be written to. Default is dup-results.txt

`-b [READ_MAX_BYTES]` : The max number of bytes to read from a file in order to create the SHA1 hash.  Default is 4096, 0 means read the whole file.

`-f [ENABLE_FAST_FINGERPRINT]` : Enable fast-fingerprint  mode.  If set to 1, then only READ_MAX_BYTES will be read to be hashed, otherwise, the whole file is hashed.

##Author

Alain Lefebvre <alain.lefebvre 'at'  mindgeek.com>

##License

Copyright 2014 MindGeek, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
