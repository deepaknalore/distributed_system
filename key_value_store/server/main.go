package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
	"flag"
	"math"
	"math/rand"

	"github.com/orcaman/concurrent-map"
	"google.golang.org/grpc"
	pb "store"
)

const letterBytes = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

var m = cmap.New()
var m1 = make(map[string]string)
var f, err = os.OpenFile("log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
var MAX_LOG_SET_COUNT = 1000000
//Stats variables start
var successfulsetcount = 0
var successfulegetcount =0
var successfulgetprefixcount = 0
//Stats variables end
// if err != nil {
// 		log.Fatal(err)
// 	}

var (
	//port = ":50051"
	port = ":56567"
	logFile string
	dataFile string
	startTime string
	setcount int
	getcount int
	getprefixcount int
)

type server struct {
	pb.KeyValueStoreServer
}

func (s *server) Set(ctx context.Context, in *pb.KeyValue) (*pb.Response, error) {

	if setcount == MAX_LOG_SET_COUNT {
		MAX_LOG_SET_COUNT = 10*MAX_LOG_SET_COUNT
		fmt.Printf("Checkpoint triggered\n")
		tempFile, tempFileErr := os.Create("temp.txt")
		if tempFileErr != nil {
		  return &pb.Response{Reply: false}, tempFileErr
		}
		defer tempFile.Close()

		var sanpBuffer bytes.Buffer
		for item := range m.Iter() {
			sanpBuffer.WriteString(item.Key+":"+item.Val.(string)+"\n")
		}
		if _, tempWriteErr := tempFile.WriteString(sanpBuffer.String()); tempWriteErr != nil {
			tempFile.Sync()
			tempFile.Close()
			log.Fatal(tempWriteErr)
		}
		exec.Command("mv", "temp.txt", "data.txt").Output()
		f, err = os.Create("log.txt")
		setcount = 0
		MAX_LOG_SET_COUNT = MAX_LOG_SET_COUNT/10
		fmt.Printf("Checkpointing completed")
	}

	key := in.GetKey()
	val := in.GetValue()

	if _, err := f.WriteString(key+":"+val+"\n"); err != nil {
		f.Close()
		log.Fatal(err)
	}
	f.Sync()
	m.Set(key, val)
	setcount = setcount + 1
	//log.Printf("Set count: %d", setcount)
	return &pb.Response{Reply: true}, nil

	// if err := f.Close(); err != nil {
	// 	log.Fatal(err)
	// }
}

func (s *server) Get(ctx context.Context, in *pb.Key) (*pb.Value, error) {
	//fmt.Printf("Received: %v\n", in.GetKey())
	var val = ""
	if tmp, ok := m.Get(in.GetKey()); ok {
		val = tmp.(string)
		//fmt.Printf("Value: %v\n", val)
	}
	getcount += 1
	return &pb.Value{Value: val}, nil
}

func (s *server) GetPrefix(in *pb.Key, stream pb.KeyValueStore_GetPrefixServer) error {
	counter := 0
	log.Printf("%d", m.Count())
	for item := range m.Iter() {
		if strings.HasPrefix(item.Key,in.GetKey()) {
			val := item.Val.(string)
			log.Printf(val)
			if err := stream.Send(&pb.Value{Value: val}); err != nil {
				return err
			}
		counter++
		}
	}	
	getprefixcount += 1
	log.Printf("Sent %d items", counter)
	return nil
}

func (s* server) GetStats(ctx context.Context, in *pb.StatRequest) (*pb.Stat, error) {
	return &pb.Stat{StartTime: startTime, SetCount: int32(setcount), GetCount: int32(getcount), GetPrefixCount: int32(getprefixcount)}, nil
}


func RestoreData() {
	// Restoring data from the checkpoint file
	data, err := ioutil.ReadFile("data.txt")
	if err == nil {
		lines := strings.Split(string(data), "\n")
		numLines := len(lines)-1
		fmt.Printf("Total number of key-value pairs in data file : %d\n", numLines)
		for i := 0; i < numLines; i++ {
			if len(lines[i]) > 0 {
				kv := strings.Split(string(lines[i]), ":")
				m.Set(string(kv[0]), string(kv[1]))
			}
		}
	}
	// Restoring data that was logged after the checkpoint
	data, err = ioutil.ReadFile("log.txt")
	if err == nil {
		lines := strings.Split(string(data), "\n")
		numLines := len(lines)-1
		fmt.Printf("Total number of key-value pairs in log file : %d\n", numLines)
		for i := 0; i < numLines; i++ {
			if len(lines[i]) > 0 {
				kv := strings.Split(string(lines[i]), ":")
				m.Set(string(kv[0]), string(kv[1]))
			}
		}
	}
	fmt.Printf("The data has restoration process is completed\n")
	fmt.Printf("The total number of key-values paris in the map is: %d\n", m.Count())
}

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func GenerateKeyValueData(dbdata float64, keysize int, valuesize int) {
	fmt.Printf("Started to generate key value data\n")
	var count = int(float64(dbdata)*math.Pow(10, 9)/(float64(valuesize)))
	//var count = 10
	fmt.Printf("The count of key values that will be generated: %s\n", string(count))
	keyfile, _ := os.Create("keys.txt")
	datafile, _ := os.Create("data.txt")
	for i := 0; i < count; i++ {
		var key = RandStringBytes(keysize)
		var val = RandStringBytes(valuesize)
		keyfile.WriteString(key + "\n")
		datafile.WriteString(key+":"+val+"\n")
	}
	keyfile.Sync()
	keyfile.Close()
	datafile.Sync()
	datafile.Close()
	fmt.Printf("Generation of key value data is complete\n")
}

func main() {

	startTime = time.Now().String()
	flag.StringVar(&logFile, "logFile", "log.txt", "-log <String> - file for writing logs")
	flag.StringVar(&dataFile, "dataFile", "data.txt", "-data <String> - file for writing data")
	flag.Parse()
	setcount = 0
	getcount = 0
	getprefixcount = 0

	fmt.Printf("\nServer started with the following info:\nServer start time: %s\nLogFile: %v\nDataFile: %v\n", startTime, logFile, dataFile)

	GenerateKeyValueData(1.0, 128, 512)
	RestoreData()
	
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	//fmt.Printf("The number of elements in the KVstore are %d", m.Count())
	// doEvery(10*time.Second, Stat)
	s := grpc.NewServer()
	pb.RegisterKeyValueStoreServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
	fmt.Printf("The number of elements in the KVstore are %d", m.Count())
}
