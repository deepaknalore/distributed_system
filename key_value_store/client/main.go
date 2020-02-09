package main

import (
        "context"
        "log"
		"time"
		"math/rand"
		"os"
		"io/ioutil"
		"strings"
		"fmt"

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

// The below function is taken from:
// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
func RandStringBytes(n int) string {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[rand.Intn(len(letterBytes))]
    }
    return string(b)
}

func GenerateKeys(keycount int) {
	keysfile, _ := os.Create("client/keys.txt")
	var key = ""
	for i := 0; i < keyCount; i++ { 
		key = RandStringBytes(4)
		keysfile.WriteString(key+"\n")
	}
	keysfile.Sync()
	keysfile.Close()
}

func WriteWorkload(c pb.KeyValueStoreClient, ctx context.Context, valuesize int) {
	data, err := ioutil.ReadFile("client/keys.txt")
	check(err)
	keys := strings.Split(string(data), "\n")
	for i := 0; i < len(keys)-1; i++ {
		if len(keys[i]) > 0 {
			var key = keys[i]
			var value = RandStringBytes(valuesize)
			setresult, seterror := c.Set(ctx, &pb.KeyValue{Key: key, Value: value})
			if seterror != nil {
				log.Printf("Key : %s", string(key))
				log.Printf("Value : %s", string(value))
				log.Fatalf("Set Failed in WriteWorkload: %v", err)
				//log.Printf("Set Failed in WriteWorkload: %v", err)
			}
			log.Printf("Greeting: %t", setresult.GetReply())
		}
	}
}

func ReadWorkload(c pb.KeyValueStoreClient, ctx context.Context, operations int) {
	var succoperations = 0
	data, err := ioutil.ReadFile("client/keys.txt")
	check(err)
	keys := strings.Split(string(data), "\n")
	//Measuring time, taken from: https://coderwall.com/p/cp5fya/measuring-execution-time-in-go
	start := time.Now()
	for i := 0; i < operations; i++ {
		result, error := c.Get(ctx, &pb.Key{Key: keys[i%len(keys)]})
		if error != nil {
			log.Fatalf("Not able to get the value for key, got error : %v", err)
		}
		if result != nil {
			succoperations = succoperations + 1
			fmt.Printf(string(result.Value) + "\n")
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("The number of successful operations is : %d\n", succoperations)
	fmt.Printf("The time taken for all the get operations is %s\n: ", elapsed)
}

func ReadUpdateWorkload(c pb.KeyValueStoreClient, ctx context.Context, valuesize int, operations int) {
	data, err := ioutil.ReadFile("client/keys.txt")
	check(err)
	keys := strings.Split(string(data), "\n")
	for i := 0; i < operations; i++ {
		rand1 := rand.Intn(2)
		rand2 := rand.Intn(len(keys))
		if rand1 == 0 {
			result, error := c.Get(ctx, &pb.Key{Key: keys[rand2]})
			if error != nil {
				log.Fatalf("Not able to get the value for key, got error : %v", err)
			}
			if result != nil {
				fmt.Printf(string(result.Value) + "\n")
			}
		} else {
			var key = keys[rand2]
			var value = RandStringBytes(valuesize)
			setresult, seterror := c.Set(ctx, &pb.KeyValue{Key: key, Value: value})
			if seterror != nil {
				log.Printf("Key : %s", string(key))
				log.Printf("Value : %s", string(value))
				log.Fatalf("Set Failed in WriteWorkload: %v", err)
			}
			log.Printf("Greeting: %t", setresult.GetReply())
		}
	}
}

func main() {

	//GenerateKeys(1000)

	conn, err := grpc.Dial(port, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKeyValueStoreClient(conn)
	key := "des2"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	//WriteWorkload(c, ctx, 40)
	ReadWorkload(c, ctx, 100)

	r, err := c.Set(ctx, &pb.KeyValue{Key: key, Value: "1"})
	if err != nil {
		log.Fatalf("Set Failed: %v", err)
	}
	log.Printf("Greeting: %t", r.GetReply())
	
	r1, err1 := c.Get(ctx, &pb.Key{Key: key})
	if err1 != nil {
                log.Fatalf("Get Failed: %v", err1)
        }
    log.Printf(string(r1.Value))
	
	// stream, err := c.GetPrefix(ctx, &pb.Key{Key: key})
	// for {
	// 	value, err2 := stream.Recv()
	// 	if err2 == io.EOF {
	// 		break
	// 	}
	// 	if err2 != nil {
	// 		log.Fatalf("GetPrefix Failed: %v", err2)
	// 	}
	// 	log.Println(value)
	// }	

	// log.Printf("Greeting: %v", r1.GetValue())
	// _, err2 := c.GetPrefix(ctx, &pb.Key{Key: key})
 //        if err2 != nil {
 //                log.Fatalf("GetPrefix Failed: %v", err2)
 //        }
	

}
