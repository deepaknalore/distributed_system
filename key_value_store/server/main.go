package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	pb "store"
)

var (
	port = ":50051"
)

type server struct {
	pb.KeyValueStoreServer
}

func (s *server) Set(ctx context.Context, in *pb.KeyValue) (*pb.Response, error) {
	log.Printf("Received: %v %v", in.GetKey(), in.GetValue())
	return &pb.Response{Reply: true}, nil
}

func (s *server) Get(ctx context.Context, in *pb.Key) (*pb.KeyValue, error) {
	log.Printf("Received: %v", in.GetKey())
	return &pb.KeyValue{Key: in.GetKey(), Value: "1"}, nil
}

func (s *server) GetPrefix(in *pb.Key, stream pb.KeyValueStore_GetPrefixServer) error {
	log.Printf("Received: %v", in.GetKey())
	return nil
}

func main() {
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
