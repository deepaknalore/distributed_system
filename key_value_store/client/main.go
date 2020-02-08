package main

import (
        "context"
        "log"
		"time"
		"io"
		"math/rand"
		"os"
		// "io/ioutil"
		// "strings"

        "google.golang.org/grpc"
        pb "store"
)

var (
        port = "localhost:50051"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func RandStringBytes(n int) string {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[rand.Intn(len(letterBytes))]
    }
    return string(b)
}

func GenerateKeys() {
	var keyCount = 1000
	keysfile, _ := os.Create("client/keys.txt")
	var key = ""
	for i := 0; i < keyCount; i++ { 
		key = RandStringBytes(4)
		keysfile.WriteString(key+"\n")
	}
	keysfile.Sync()
	keysfile.Close()
}

// func WriteData() {
// 	for i := 0; i < len(keyCount); i++ { 
// 		key = 
// 	}
// }

func ReadWorkload(c *, ctx) {
	var operations = 100
	data, err := ioutil.ReadFile("client/keys.txt")
	check(err)
	keys := strings.Split(string(data), "\n")
	for i := 0; i < operations; i++ {
		result, error := c.Get(ctx, &pb.Key{Key: keys[i%len(keys)]})
	}
}

func main() {

	GenerateKeys()

	conn, err := grpc.Dial(port, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKeyValueStoreClient(conn)
	key := "des2"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()


	ReadWorkload(c, ctx)

	r, err := c.Set(ctx, &pb.KeyValue{Key: key, Value: "1"})
	if err != nil {
		log.Fatalf("Set Failed: %v", err)
	}
	log.Printf("Greeting: %t", r.GetReply())
	
	r1, err1 := c.Get(ctx, &pb.Key{Key: key})
	if err1 != nil {
                log.Fatalf("Get Failed: %v", err1)
        }
	
	stream, err := c.GetPrefix(ctx, &pb.Key{Key: key})
	for {
		value, err2 := stream.Recv()
		if err2 == io.EOF {
			break
		}
		if err2 != nil {
			log.Fatalf("GetPrefix Failed: %v", err2)
		}
		log.Println(value)
	}	

	log.Printf("Greeting: %v", r1.GetValue())
	_, err2 := c.GetPrefix(ctx, &pb.Key{Key: key})
        if err2 != nil {
                log.Fatalf("GetPrefix Failed: %v", err2)
        }
	

}
