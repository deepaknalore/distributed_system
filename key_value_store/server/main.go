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

	"github.com/orcaman/concurrent-map"
	"google.golang.org/grpc"
	pb "store"
)

var m = cmap.New()
var m1 = make(map[string]string)
var f, err = os.OpenFile("log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
var MAX_LOG_SET_COUNT = 20000
//Stats variables start
var successfulsetcount = 0
var successfulegetcount =0
var successfulgetprefixcount = 0
//Stats variables end
// if err != nil {
// 		log.Fatal(err)
// 	}

var (
	port = ":50051"
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
		tempFile, tempFileErr := os.Create("temp.txt")
		if tempFileErr != nil {
		  return &pb.Response{Reply: false}, tempFileErr
		}
		defer tempFile.Close()

		var sanpBuffer bytes.Buffer
		for item := range m.Iter() {
			key := item.Key
			val := item.Val.(string)
			sanpBuffer.WriteString(key+":"+val+"\n")
		}
		if _, tempWriteErr := tempFile.WriteString(sanpBuffer.String()); tempWriteErr != nil {
			tempFile.Close()
			log.Fatal(tempWriteErr)
		}
		exec.Command("mv", "temp.txt", "data.txt").Output()
		f, err = os.Create("log.txt")
		setcount = 0
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
	fmt.Printf("Received: %v", in.GetKey())
	var val = ""
	if tmp, ok := m.Get(in.GetKey()); ok {
		val = tmp.(string)
		fmt.Printf("Value: %v\n", val)
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

// Taken from https://gist.github.com/ryanfitz/4191392
func doEvery(d time.Duration, f func(time.Time)) {
	for x := range time.Tick(d) {
		f(x)
	}
}

func Stat(t time.Time) {
	fmt.Printf("The total number of Get operations are: %d\n", getcount)
	fmt.Printf("The total number of Set operations are: %d\n", setcount)
	fmt.Printf("The total number of getPrefix operations are: %d\n", getprefixcount)
}

func RestoreData() {
	// Restoring data from the checkpointed
	// How to handle error where there is not data in DataFile is not clear
	data, err := ioutil.ReadFile("server/data.txt")
	log.Printf(string(data))
	if err == nil {
		lines := strings.Split(string(data), "\n")
		for i := 0; i < len(lines); i++ { 
			if len(lines[i]) > 0 {
				kv := strings.Split(string(lines[i]), ":")
				// fmt.Println(kv[0])
				// fmt.Println(kv[1])
				m.Set(string(kv[0]), string(kv[1]))
			}
		}
	}
	// Restoring data that was logged after the checkpoing was done
	data, err = ioutil.ReadFile("server/log.txt")
	if err == nil {
		lines := strings.Split(string(data), "\n")
		for i := 0; i < len(lines); i++ { 
			if len(lines[i]) > 0 {
				kv := strings.Split(string(lines[i]), ":")
				// fmt.Println(kv[0])
				// fmt.Println(kv[1])
				m.Set(string(kv[0]), string(kv[1]))
			}
		}
	}
}

func main() {

	startTime = time.Now().String()
	flag.StringVar(&logFile, "logFile", "log.txt", "-log <String> - file for writing logs")
	flag.StringVar(&dataFile, "dataFile", "data.txt", "-data <String> - file for writing data")
	flag.Parse()
	setcount = 0
	getcount = 0
	getprefixcount = 0

	log.Printf("\nServer started with the following info:\n LogFile: %v\n DataFile: %v\n", logFile,dataFile)

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
