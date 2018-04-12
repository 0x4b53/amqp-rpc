package main

import (
	"context"
	"fmt"

	"github.com/bombsimon/amqp-rpc/logger"
	rmq_server "github.com/bombsimon/amqp-rpc/server"

	"github.com/streadway/amqp"
)

func main() {
	server := rmq_server.New()

	server.AddHandler("hello_world", handleHelloWorld)

	server.ListenAndServe("amqp://guest:guest@localhost:5672/")
}

func handleHelloWorld(c context.Context, d *amqp.Delivery) []byte {
	logger.Infof("Handling 'Hello world' request")

	return []byte(fmt.Sprintf("Got message: %s", d.Body))
}
