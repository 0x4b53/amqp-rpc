package main

import (
	"context"
	"fmt"
	"log"
	"os"

	amqprpc "github.com/0x4b53/amqp-rpc/v4"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	url    = "amqps://guest:guest@localhost:5672/"
	logger = log.New(os.Stdout, "[amqp-rpc]", log.LstdFlags)
)

func main() {
	cert := amqprpc.Certificates{
		CA: "cacert.pem",
	}

	s := amqprpc.NewServer(url).WithDialConfig(amqp.Config{
		TLSClientConfig: cert.TLSConfig(),
	})
	s.WithErrorLogger(logger.Printf)

	s.Bind(amqprpc.DirectBinding("hello_world", handleHelloWorld))
	s.Bind(amqprpc.DirectBinding("client_usage", handleClientUsage))

	s.ListenAndServe()
}

func handleHelloWorld(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	logger.Printf("Handling 'Hello world' request")

	fmt.Fprintf(rw, "Got message: %s", d.Body)
}

func handleClientUsage(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	logger.Printf("Handling 'Client usage' request")

	cert := amqprpc.Certificates{
		CA: "cacert.pem",
	}

	c := amqprpc.NewClient("amqps://guest:guest@localhost:5671/").
		WithTLS(cert.TLSConfig())

	request := amqprpc.NewRequest().
		WithRoutingKey("hello_world").
		WithBody("Sent with client")

	response, err := c.Send(request)
	if err != nil {
		logger.Printf("Something went wrong: %s", err)
		fmt.Fprint(rw, err.Error())
	}

	fmt.Fprintf(rw, "Response from next endpoint: %s", response.Body)
}
