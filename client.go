package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"google.golang.org/grpc"
	"net/rpc/jsonrpc"
	

	"github.com/ardanlabs/python-go/grpc/pb"
)

type Data struct {
	Plugin_type           string
	Local_resource_Source string
}

func main() {
	// RPC CALL
	client1, err := jsonrpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("Error connecting to RPC server:", err)
	}
	var data Data
	err = client1.Call("RPCMethods.GetData", "", &data)
	if err != nil {
		log.Fatal("RPC call error:", err)
	}
	fmt.Println("Received data:", data)

	// GRPC CALL
	addr := "localhost:9999"
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewOutliersClient(conn)
	req := &pb.OutliersRequest{
		Metrics: dummyData(data),
	}

	resp, err := client.Detect(context.Background(), req)
	if err != nil {
		log.Fatal(err)
	}
	jsonString, _ := json.Marshal(string(resp.Indices))
	ioutil.WriteFile("big_marhsall.json", jsonString, os.ModePerm)

	log.Printf("outliers at: %v", string(resp.Indices))
}

func dummyData(data Data) []*pb.Metric {
	const size = 1
	out := make([]*pb.Metric, size)
	for i := 0; i < size; i++ {
		m := pb.Metric{
			Name: data.Local_resource_Source,
		}
		out[i] = &m
	}
	fmt.Println(out)
	return out
}

