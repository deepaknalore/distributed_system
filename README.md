# distributed_system
The guide helps you to setup gRPC, go and protobuf on your system. Contains important guide to understand gRPC, go and protobuf.
  
Setup Guide:
1. Install go using:
brew install go
2. Install protobuf using:
brew install protobuf

Later, follow the guide from:
https://grpc.io/docs/quickstart/go/ ---> Also try the example to understand whether everything works as expected.
to install gRPC and skip the part where you download and install protobuf.

Important information related to paths:
Make sure the path is set as follows for it to work.
export GOROOT=/usr/local/go
export GOPATH=$HOME/go
export GOBIN=$GOPATH/bin
export PATH=$PATH:$GOROOT:$GOPATH:$GOBIN


Important links to understand the underlying concepts:
https://grpc.io/docs/guides/
https://grpc.io/docs/guides/concepts/
https://grpc.io/docs/tutorials/basic/go/
https://godoc.org/google.golang.org/grpc
