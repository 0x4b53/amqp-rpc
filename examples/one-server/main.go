package main

import (
	"context"
	"fmt"

	"github.com/bombsimon/amqp-rpc/logger"
	"github.com/bombsimon/amqp-rpc/server"

	"github.com/streadway/amqp"
)

func main() {
	s := server.New("amqp://guest:guest@localhost:5672/")

	s.AddHandler("hello_world", handleHelloWorld)

	s.ListenAndServe()
}

func handleHelloWorld(c context.Context, d amqp.Delivery) []byte {
	logger.Infof("Handling 'Hello world' request")

	return []byte(fmt.Sprintf("Got message: %s", d.Body))
}
