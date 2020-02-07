package main

import (
	"context"
	"log"
	"net"
	"fmt"
	"io/ioutil"
	"strings"

	"google.golang.org/grpc"
	"github.com/orcaman/concurrent-map"
	pb "store"
)

var m1 = make(map[string]string)
var m = cmap.New()

var (
	port = ":50051"
)

type server struct {
	pb.KeyValueStoreServer
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func (s *server) Set(ctx context.Context, in *pb.KeyValue) (*pb.Response, error) {
	key := in.GetKey()
	val := in.GetValue()
	m.Set(key, val)
	return &pb.Response{Reply: true}, nil
}

func (s *server) Get(ctx context.Context, in *pb.Key) (*pb.Value, error) {
	log.Printf("Received: %v", in.GetKey())
	var bar = "Bar"
	if tmp, ok := m.Get(in.GetKey()); ok {
		bar = tmp.(string)
		log.Printf("Cmap value: %v", bar)
	}
	return &pb.Value{Value: "The value is " + bar}, nil
}

func (s *server) GetPrefix(in *pb.Key, stream pb.KeyValueStore_GetPrefixServer) error {
	log.Printf("Received: %v", in.GetKey())
	return nil
}

func main() {
	m := cmap.New()
	m1["Hello"] = "world"
	data, err := ioutil.ReadFile("server/datafile.txt")
	check(err)
	lines := strings.Split(string(data), "\n")
	for i := 0; i < len(lines); i++ { 
		kv := strings.Split(string(lines[i]), ":")
		fmt.Println(kv[0])
		fmt.Println(kv[1])
		m.Set(string(kv[0]), string(kv[1]))
	}
	
	kv := strings.Split(string(lines[0]), ":")
	fmt.Println(kv[0])
	fmt.Println(kv[1])
	key := kv[0]
	val := kv[1]
	m.Set(key, val)
	

	temp, okk := m.Get("foo")
	fmt.Println(temp)
	fmt.Println(okk)

	if tmp, ok := m.Get("Lion"); ok {
		bar := tmp.(string)
		log.Printf("Cmap value: %v", bar)
	}

	m.Remove("foo")

	
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKeyValueStoreServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
