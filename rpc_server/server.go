package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
)

type Data struct {
	Plugin_type           string
	Local_resource_Source string
}

type RPCMethods struct{}

func (m *RPCMethods) GetData(args interface{}, reply *Data) error {
	// Prepare the data to be sent to the client
	data := Data{
		Plugin_type:           "cmf",
		Local_resource_Source: "/home/vboxuser/Documents/mlmd_json/py/mlmd",
	}

	*reply = data
	return nil
}

func main() {
	rpcMethods := new(RPCMethods)
	rpc.Register(rpcMethods)

	// Start the RPC server
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("Error starting RPC server:", err)
	}

	fmt.Println("RPC server started on port 1234")

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("Error accepting connection:", err)
		}
		go jsonrpc.ServeConn(conn)
	}
}