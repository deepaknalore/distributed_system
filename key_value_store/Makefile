# Go Parameters
GOCMD=go
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOPATH=$(shell go env GOPATH)
GOBIN=$(shell go env GOBIN)
GOROOT=$(shell go env GOROOT)
PROTOC := $(shell which protoc)

all: setup
server:
	go run server/main.go
	
client:
	go run client/go
	
setup:
	protoc -I store/ store/key_value.proto --go_out=plugins=grpc:$(GOROOT)/src/store
	
deps:
	$(GOGET) github.com/markbates/goth
	$(GOGET) github.com/markbates/pop
