# Distributed_system
The guide helps you to setup gRPC, go and protobuf on your system. Contains important guide to understand gRPC, go and protobuf.
  
Setup Guide:
1. Install go using:
brew install go
2. Install protobuf using:
brew install protobuf

Later, follow the guide from:
https://grpc.io/docs/quickstart/go/ ---> Also try the example to understand whether everything works as expected.
to install gRPC and skip the part where you download and install protobuf.

In order to compile the proto file:
protoc -I store/ store/key_value.proto --go_out=plugins=grpc:store

Place the generated pb.go file in:
	/usr/local/go/src/store (from $GOROOT)
	/Users/dsrinath/go/src/store (from $GOPATH)
The above is an important prerequiste for the MAKE file to work

Important information related to paths:


Make sure the path is set as follows for it to work.


* export GOROOT=/usr/local/go
* export GOPATH=$HOME/go
* export GOBIN=$GOPATH/bin
* export PATH=$PATH:$GOROOT:$GOPATH:$GOBIN

Important links to understand the underlying concepts:


1. https://grpc.io/docs/guides/
2. https://grpc.io/docs/guides/concepts/
3. https://grpc.io/docs/tutorials/basic/go/
4. https://godoc.org/google.golang.org/grpc

# Usage:
Procedure to build the files:
* cd to key_value_store
* run the command "make"

Script usage:

* This is to be used to automatically run the server and required number of clients and dump the read update stats, to see that the functionality is correct. By default every client runs for a maximum of 3 minutes.
* ./executor <number of clients>

Server:
* ./kvserver -logFile log.txt -dataFile data.txt ()
* ./kvserver -logFile log.txt -dataFile data.txt -generatenewdata 1 -dbsize 4.0 -valuesize 4096 

*logFile - Location of the log file
*dataFile - Location of the data file
*generatenewdata - Boolean parameter to generate new data
*dbSize - For initializing the database, should be passed along with generatenewdata
*valueSize - Size of the values
By default all the key size is 128 Bytes

Client:
* ./kvclient -operation read (To start the read workload)
* ./kvclient -operation read_update -valueSize 4000 (To start the read_update workload)
* ./kvclient -operation get_prefix_test -prefixSize 128 -operationCount 1 (To run the test for getPrefix functionlaity)
* ./kvclient -operation write -valueSize 4096 -operationCount 100 (To start the write workload)
* ./kvclient -operation stat (To get the server stats)

*operationCount - The number of operations for read/write/etc
*prefixsize - The prefix size to be used for getprefixtest









