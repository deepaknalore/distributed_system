package main

import (
        "context"
        "log"
	"time"

        "google.golang.org/grpc"
        pb "store"
)

var (
        port = "localhost:50051"
)

func main() {

	conn, err := grpc.Dial(port, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKeyValueStoreClient(conn)
	key := "des2"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.Set(ctx, &pb.KeyValue{Key: key, Value: "1"})
	if err != nil {
		log.Fatalf("Set Failed: %v", err)
	}
	log.Printf("Greeting: %t", r.GetReply())
	
	r1, err1 := c.Get(ctx, &pb.Key{Key: key})
	if err1 != nil {
                log.Fatalf("Get Failed: %v", err1)
        }
	
	log.Printf("Greeting: %v", r1.GetValue())
	_, err2 := c.GetPrefix(ctx, &pb.Key{Key: key})
        if err2 != nil {
                log.Fatalf("GetPrefix Failed: %v", err2)
        }
	

}
