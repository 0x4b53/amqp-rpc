package main

import (
	"context"
	"fmt"

	rmq_server "github.com/bombsimon/amqp-rpc/server"
	"github.com/streadway/amqp"
)

func main() {
	server := rmq_server.New()

	server.AddHandler("hello_world", handleHelloWorld)

	server.ListenAndServe()
}

func handleHelloWorld(c context.Context, d *amqp.Delivery) []byte {
	fmt.Println("Handling 'Hello world' request")

	return []byte(fmt.Sprintf("Got message: %s", d.Body))
}
