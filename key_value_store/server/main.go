package main

import (
	"context"
	"log"
	"net"
	"fmt"
	"io/ioutil"
	"strings"
	"os"
	"os/exec"
	"bytes"

	"google.golang.org/grpc"
	"github.com/orcaman/concurrent-map"
	pb "store"
)

var m = cmap.New()
var m1 = make(map[string]string)
var setcount = 0
var f, err = os.OpenFile("server/log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
var MAX_LOG_SET_COUNT = 20000
// if err != nil {
// 		log.Fatal(err)
// 	}

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

	if setcount == MAX_LOG_SET_COUNT {
		tempFile, tempFileErr := os.Create("server/temp.txt")
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
		exec.Command("mv", "server/temp.txt", "server/datafile.txt").Output()
		f, err = os.Create("server/log.txt")
		setcount = 0
	}

	key := in.GetKey()
	val := in.GetValue()

	var buffer bytes.Buffer
	buffer.WriteString(key)
	buffer.WriteString(":")
	buffer.WriteString(val)
	buffer.WriteString("\n")
	if _, err := f.WriteString(buffer.String()); err != nil {
		f.Close()
		log.Fatal(err)
	}
	//os.File.Sync() 
	f.Sync()
	m.Set(key, val)
	setcount = setcount + 1
	log.Printf("Set count: %d", setcount)
	return &pb.Response{Reply: true}, nil

	// if err := f.Close(); err != nil {
	// 	log.Fatal(err)
	// }
}

func (s *server) Get(ctx context.Context, in *pb.Key) (*pb.Value, error) {
	fmt.Printf("Received: %v", in.GetKey())
	var val = "DefaultValue"
	if tmp, ok := m.Get(in.GetKey()); ok {
		val = tmp.(string)
		fmt.Printf("Value: %v\n", val)
	}
	return &pb.Value{Value: "The value is " + val}, nil
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

	log.Printf("Sent %d items", counter)
	return nil
}

func RestoreData() {
	// Restoring data from the checkpointed DataFile
	// How to handle error where there is not data in DataFile is not clear
	data, err := ioutil.ReadFile("server/datafile.txt")
	check(err)
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
	check(err)
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

	RestoreData()
	
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Printf("The number of elements in the KVstore are %d", m.Count())
	s := grpc.NewServer()
	pb.RegisterKeyValueStoreServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
	log.Printf("The number of elements in the KVstore are %d", m.Count())
}
