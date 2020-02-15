package main

import (
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	pb "store"
	"strings"
	"time"
)

var (
        port = "localhost:50051"
		iteration int
        keySize int
        valueSize int
        dbSize int
        operation string
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
	for i := 0; i < keycount; i++ { 
		key = RandStringBytes(4)
		keysfile.WriteString(key+"\n")
	}
	keysfile.Sync()
	keysfile.Close()
}

func WriteWorkload(c pb.KeyValueStoreClient, ctx context.Context, valuesize int, operations int) {
	var succoperations = 0
	data, err := ioutil.ReadFile("client/keys.txt")
	check(err)
	keys := strings.Split(string(data), "\n")
	writes := len(keys)
	if operations > writes {
		writes = operations
	}
	totalkeys := len(keys)
	start := time.Now()
	for i := 0; i < writes; i++ {
		if len(keys[i%totalkeys]) > 0 {
			setresult, seterror := c.Set(ctx, &pb.KeyValue{Key: keys[i%totalkeys], Value: RandStringBytes(valuesize)})
			if seterror != nil {
				//log.Printf("Key : %s", string(key))
				//log.Printf("Value : %s", string(value))
				log.Fatalf("Set Failed in WriteWorkload: %v", err)
				//log.Printf("Set Failed in WriteWorkload: %v", err)
			}
			if setresult.GetReply() == true {
				succoperations += 1
			}
			log.Printf("Greeting: %t", setresult.GetReply())
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("The number of successful Writes is : %d\n", succoperations)
	fmt.Printf("The time taken for all the Write operations is %s\n: ", elapsed)
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
	fmt.Printf("The number of successful Reads is : %d\n", succoperations)
	fmt.Printf("The time taken for all the get operations is %s\n: ", elapsed)
}

func ReadUpdateWorkload(c pb.KeyValueStoreClient, ctx context.Context, valuesize int, operations int) {
	var readcount = 0
	var updatecount = 0
	data, err := ioutil.ReadFile("client/keys.txt")
	check(err)
	keys := strings.Split(string(data), "\n")
	start := time.Now()
	for i := 0; i < operations; i++ {
		rand1 := rand.Intn(2)
		rand2 := rand.Intn(len(keys))
		if rand1 == 0 {
			result, error := c.Get(ctx, &pb.Key{Key: keys[rand2]})
			if error != nil {
				log.Fatalf("Not able to get the value for key, got error : %v", err)
			}
			if result != nil {
				readcount += 1
				fmt.Printf(string(result.Value) + "\n")
			}
		} else {
			setresult, seterror := c.Set(ctx, &pb.KeyValue{Key: keys[rand2], Value: RandStringBytes(valuesize)})
			if seterror != nil {
				//log.Printf("Key : %s", string(key))
				//log.Printf("Value : %s", string(value))
				log.Fatalf("Set Failed in WriteWorkload: %v", err)
			}
			if setresult.GetReply() == true {
				updatecount += 1
			}
			log.Printf("Greeting: %t", setresult.GetReply())
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("The number of successful Reads is : %d\n", readcount)
	fmt.Printf("The number of successful Updates is : %d\n", updatecount)
	fmt.Printf("The time taken for all the operations is %s\n: ", elapsed)
}

func main() {

	flag.IntVar(&iteration, "iter", 1234, "-iter <int>")
	flag.IntVar(&keySize, "keySize", 4, "-keySize <int> in terms of bytes ")
	flag.IntVar(&valueSize, "valueSize", 10, "-valueSize <int> in terms of bytes")
	flag.IntVar(&dbSize, "dbSize", 10, "-dbSize <int> in terms of GB")
	flag.StringVar(&operation, "operation", "read", "-operation <String> - read,read_write,write,stats")
	flag.Parse()
	//GenerateKeys(1000)
	log.Printf("\nClient started with the following info:\n DB-Size: %d \n Number of iterations: %d\n Key Size: " +
		"%d\n Value Size: %d\n Operation: %v", dbSize, iteration,keySize,valueSize,operation)
	conn, err := grpc.Dial(port, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKeyValueStoreClient(conn)
	key := "des2"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch operation {
	case "read":
		ReadWorkload(c, ctx, 10000)
	case "read_write":
		ReadUpdateWorkload(c, ctx, 40, 1000)
	case "write":
		//WriteWorkload(c, ctx, 40)
	}

	r, err := c.Set(ctx, &pb.KeyValue{Key: key, Value: "10"})
	if err != nil {
		log.Fatalf("Set Failed: %v", err)
	}
	log.Printf("Greeting: %t", r.GetReply())
	
	r1, err1 := c.Get(ctx, &pb.Key{Key: "des2"})
	if err1 != nil {
                log.Fatalf("Get Failed: %v", err1)
        }
    fmt.Printf("%s\n",string(r1.Value))

	stat, err := c.GetStats(ctx, &pb.StatRequest{})
	if err!= nil {
		log.Fatalf("Stat retrieval failed: %v", err)
	}
	log.Printf("\n\nStats:\n Server- Start time: %v \n Set-Count : %d\n Get-count : %d\n GetPrefix-count : %d\n",
		stat.StartTime, stat.SetCount, stat.GetCount, stat.GetPrefixCount)
	
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
