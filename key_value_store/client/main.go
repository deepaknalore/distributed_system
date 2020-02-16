package main

import (
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"math/rand"
	"math"
	"os"
	pb "store"
	"strings"
	"time"
	"io"
)

var (
        //port = "localhost:50051"
        port = "10.10.1.2:56567"
        keySize int
        valueSize int
        dbSize float64
        operation string
        opCount int
		prefixSize int
)

const letterBytes = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

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

func WriteWorkload(c pb.KeyValueStoreClient, ctx context.Context, valuesize int, operations int) {
	var f, _ = os.Create("durability.txt")
	var succoperations = 0
	data, err := ioutil.ReadFile("keys.txt")
	check(err)
	keys := strings.Split(string(data), "\n")
	writes := operations
	fmt.Printf("The number of writes that will be performed : %d\n", writes)
	totalkeys := len(keys)
	start := time.Now()
	for i := 0; i < writes; i++ {
		var rand1 = rand.Intn(totalkeys-1)
		var key = keys[rand1%totalkeys]
		if len(key) > 0 {
			//fmt.Printf("key : %s", key)
			var val = RandStringBytes(valuesize)
			f.WriteString(key+":"+val+"\n")
			f.Sync()
			setresult, seterror := c.Set(ctx, &pb.KeyValue{Key: key, Value: val})
			if seterror != nil {
				//log.Printf("Key : %s", string(key))
				//log.Printf("Value : %s", string(value))
				//log.Fatalf("Set Failed in WriteWorkload: %v", err)
				//log.Printf("Set Failed in WriteWorkload: %v", err)
			}
			if setresult.GetReply() == true {
				f.WriteString("Above set was successfule")
				f.Sync()
				succoperations += 1
			}
			log.Printf("Greeting: %t", setresult.GetReply())
		}
	}
	f.Close()
	elapsed := time.Since(start)
	fmt.Printf("The number of successful Writes is : %d\n", succoperations)
	fmt.Printf("The time taken for all the Write operations is : %s\n", elapsed)
}

func GetPrefixTest(c pb.KeyValueStoreClient, ctx context.Context, prefixsize int,  operations int) {
	var valuecount = 0
	var getprefixcount = 0
	var start = time.Now()
	var getprefixstarttime = time.Now()
	var operationcount = 0
	var getprefixtime = time.Duration(0)
	for time.Since(start) < time.Duration(3*time.Minute) && operationcount < operations {
		operationcount += 1
		var key = RandStringBytes(prefixsize)
		fmt.Printf(key)
		getprefixstarttime = time.Now()
		stream, err := c.GetPrefix(ctx, &pb.Key{Key: key})
		if err == nil {
			getprefixcount += 1
		}
		for {
			_, err2 := stream.Recv()
			if err2 == io.EOF {
				break
			}
			if err2 != nil {
				log.Fatalf("GetPrefix Failed: %v", err2)
			}
			//fmt.Printf("%s\n", value)
			valuecount += 1
		}
		getprefixtime += time.Since(getprefixstarttime)
	}
	fmt.Printf("Total number of operations performed is : %d\n", operationcount)
	fmt.Printf("The number of successful getPrefix operations is : %d\n", getprefixcount)
	fmt.Printf("Get prefix latency : %s\n", time.Duration(int64(getprefixtime)/int64(getprefixcount)))
	fmt.Printf("Average number of values returned is : %d\n", int(float64(valuecount)/math.Max(float64(getprefixcount), 1.0)))
}

func ReadWorkload(c pb.KeyValueStoreClient, ctx context.Context, operations int) {
	var succoperations = 0
	var readtime = time.Duration(0)
	data, err := ioutil.ReadFile("keys.txt")
	check(err)
	keys := strings.Split(string(data), "\n")
	//Measuring time, taken from: https://coderwall.com/p/cp5fya/measuring-execution-time-in-go
	start := time.Now()
	var readstart = time.Now()
	//for i := 0; i < operations; i++ {
	for time.Since(start) < time.Duration(3*time.Minute) {
		readstart = time.Now()
		result, error := c.Get(ctx, &pb.Key{Key: keys[rand.Intn(len(keys)-1)]})
		if error != nil {
			//log.Fatalf("Not able to get the value for key %v, got error : %v", keys[rand], err)
		}
		if result != nil {
			readtime += time.Since(readstart)
			succoperations = succoperations + 1
			//fmt.Printf(string(result.Value) + "\n")
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("The number of successful Reads is : %d\n", succoperations)
	fmt.Printf("Expreiment elapsed time : %s\n", elapsed)
	fmt.Printf("The time taken for all the read operations is : %s\n", readtime)
	fmt.Printf("Read latency : %s\n", time.Duration(int64(readtime)/int64(succoperations)))
}

func ReadUpdateWorkload(c pb.KeyValueStoreClient, ctx context.Context, valuesize int, operations int) {
	var readcount = 0
	var updatecount = 0
	var readtime = time.Duration(0)
	var updatetime = time.Duration(0)
	data, err := ioutil.ReadFile("keys.txt")
	check(err)
	keys := strings.Split(string(data), "\n")
	start := time.Now()
	var readstart = time.Now()
	var updatestart = time.Now()
	for time.Since(start) < time.Duration(6*time.Minute) {
		rand1 := rand.Intn(2)
		rand2 := rand.Intn(len(keys)-1)
		if rand1 == 0 {
			readstart = time.Now()
			result, error := c.Get(ctx, &pb.Key{Key: keys[rand2]})
			if error != nil {
				//log.Fatalf("Not able to get the value for key, got error : %v", err)
			}
			if result != nil {
				readtime += time.Since(readstart)
				readcount += 1
				//fmt.Printf(string(result.Value) + "\n")
			}
		} else {
			updatestart = time.Now()
			setresult, seterror := c.Set(ctx, &pb.KeyValue{Key: keys[rand2], Value: RandStringBytes(valuesize)})
			if seterror != nil {
				//fmt.Printf("Key : %s", string(keys[rand2]))
				//log.Fatalf("Set Failed in WriteWorkload: %v", err)
			}
			if setresult.GetReply() == true {
				updatetime += time.Since(updatestart)
				updatecount += 1
			}
			//fmt.Printf("Greeting: %t", setresult.GetReply())
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("The number of successful Reads is : %d\n", readcount)
	fmt.Printf("The number of successful Updates is : %d\n", updatecount)
	fmt.Printf("The time taken for all the operations is : %s\n", elapsed)
	fmt.Printf("The time taken for read operations is : %s\n", readtime)
	fmt.Printf("The time taken for write operations is : %s\n", updatetime)
	fmt.Printf("Read latency : %s\n", time.Duration(int64(readtime)/int64(readcount)))
	fmt.Printf("Update latency : %s\n", time.Duration(int64(updatetime)/int64(updatecount)))
	fmt.Printf("Overall latency : %s\n", time.Duration(int64(elapsed)/int64(readcount+updatecount)))
}

func GenerateKeyData(dbdata float64, keysize int, valuesize int) {
	var count = int(float64(dbdata)*math.Pow(10, 9)/(float64(valuesize)))
	//var count = 10
	fmt.Printf(string(count))
	keyfile, _ := os.Create("keys.txt")
	var key = ""
	for i := 0; i < count; i++ {
		key = RandStringBytes(keysize)
		keyfile.WriteString(key + "\n")
	}
	keyfile.Sync()
	keyfile.Close()
	fmt.Printf("Generated the key data\n")
}

func main() {
	//https://stackoverflow.com/questions/8288679/difficulty-with-go-rand-package
	rand.Seed(time.Now().UnixNano())

	flag.IntVar(&keySize, "keySize", 128, "-keySize <int> in terms of bytes ")
	flag.IntVar(&valueSize, "valueSize", 512, "-valueSize <int> in terms of bytes")
	flag.Float64Var(&dbSize, "dbSize", 1.0, "-dbSize <float64> in terms of GB")
	flag.IntVar(&opCount, "operationCount", 1000, "-operationCount <int>")
	flag.IntVar(&prefixSize, "prefixSize", 4, "-prefixSize <int>")
	flag.StringVar(&operation, "operation", "read", "-operation <String> - read,read_write,write,stats")
	flag.Parse()

	log.Printf("\nClient started with the following info:\nDBSize: %f GB \nKey Size: " +
		"%d bytes\nValue Size: %d bytes\nOperation: %v", dbSize, keySize, valueSize, operation)
	conn, err := grpc.Dial(port, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKeyValueStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	switch operation {
	case "generate_data":
		GenerateKeyData(dbSize, keySize, valueSize)
		WriteWorkload(c, ctx, valueSize, 10)
	case "read":
		ReadWorkload(c, ctx, opCount)
	case "read_update":
		ReadUpdateWorkload(c, ctx, valueSize, opCount)
	case "write":
		WriteWorkload(c, ctx, valueSize, opCount)
	case "get_prefix_test":
		GetPrefixTest(c, ctx, prefixSize, opCount)
	}

	//key := "des2"
	//Test for working of Set
	//r, err := c.Set(ctx, &pb.KeyValue{Key: key, Value: "10"})
	//if err != nil {
	//	log.Fatalf("Set Failed: %v", err)
	//}
	//log.Printf("Greeting: %t", r.GetReply())

	//Test for working of Get
	//r1, err1 := c.Get(ctx, &pb.Key{Key: "des2"})
	//if err1 != nil {
    //            log.Fatalf("Get Failed: %v", err1)
    //    }
    //fmt.Printf("%s\n",string(r1.Value))

    //Getting the server stats at the end of all the operations
	stat, err := c.GetStats(ctx, &pb.StatRequest{})
	if err!= nil {
		log.Fatalf("Stat retrieval failed: %v", err)
	}
	fmt.Printf("\n\nStats:\nServer Start time: %v \nSet-Count : %d\nGet-count : %d\nGetPrefix-count : %d\n",
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
