# Go Parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOPATH=$(shell go env GOPATH)
GOBIN=$(shell go env GOBIN)
GOROOT=$(shell go env GOROOT)
SERVERBINARY:=kvserver
CLIENTBINARY:=kvclient
PROTOC := $(shell which protoc)
PROTOC_GEN_GO := $(GOROOT)/bin/protoc-gen-go

.PHONY: setup server client
all: server client
server:
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(SERVERBINARY) server/main.go
client:
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(CLIENTBINARY) client/main.go
setup:
	go get -u github.com/golang/protobuf/protoc-gen-go
	mkdir $(GOBIN)/src/store
	$(PROTOC) -I store/ store/key_value.proto --go_out=plugins=grpc:$(GOPATH)/src/store
	$(GOCMD) install store/key_value.pb.go
