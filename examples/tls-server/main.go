package main

import (
	"context"
	"fmt"
	"log"
	"os"

	rpcclient "github.com/bombsimon/amqp-rpc/client"
	rpcconn "github.com/bombsimon/amqp-rpc/connection"
	rpclogger "github.com/bombsimon/amqp-rpc/logger"
	rpcserver "github.com/bombsimon/amqp-rpc/server"

	"github.com/streadway/amqp"
)

func main() {
	customLogger := log.New(os.Stdout, "[amqp-rpc]", log.LstdFlags)
	rpclogger.SetInfoLogger(customLogger)
	rpclogger.SetWarnLogger(customLogger)

	server := rpcserver.New(rpcconn.Certificates{
		Cert: "server.crt",
		Key:  "server.key",
	})

	server.AddHandler("hello_world", handleHelloWorld)
	server.AddHandler("client_usage", handleClientUsage)

	server.ListenAndServe("amqps://guest:guest@localhost:5671/")
}

func handleHelloWorld(c context.Context, d *amqp.Delivery) []byte {
	rpclogger.Infof("Handling 'Hello world' request")

	return []byte(fmt.Sprintf("Got message: %s", d.Body))
}

func handleClientUsage(c context.Context, d *amqp.Delivery) []byte {
	rpclogger.Infof("Handling 'Client usage' request")

	client := rpcclient.New("amqps://guest:guest@localhost:5671/", rpcconn.Certificates{
		Cert: "client/cert.pem",
		Key:  "client/key.pem",
		CA:   "client/cacert.pem",
	})

	response, err := client.Publish("hello_world", []byte("Sent with client"), true)
	if err != nil {
		rpclogger.Warnf("Something went wrong: %s", err)
		return []byte(err.Error())
	}

	return []byte(fmt.Sprintf("Response from next endpoint: %s", response.Body))
}
