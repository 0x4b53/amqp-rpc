package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/bombsimon/amqp-rpc/client"
	"github.com/bombsimon/amqp-rpc/connection"
	"github.com/bombsimon/amqp-rpc/logger"
	"github.com/bombsimon/amqp-rpc/server"

	"github.com/streadway/amqp"
)

var url = "amqp://guest:guest@localhost:5672/"

func main() {
	customLogger := log.New(os.Stdout, "[amqp-rpc]", log.LstdFlags)
	logger.SetInfoLogger(customLogger)
	logger.SetWarnLogger(customLogger)

	cert := connection.Certificates{
		Cert: "server.crt",
		Key:  "server.key",
	}

	s := server.New(url).WithDialConfig(amqp.Config{
		TLSClientConfig: cert.TLSConfig(),
	})

	s.AddHandler("hello_world", handleHelloWorld)
	s.AddHandler("client_usage", handleClientUsage)

	s.ListenAndServe()
}

func handleHelloWorld(ctx context.Context, d *amqp.Delivery) []byte {
	logger.Infof("Handling 'Hello world' request")

	return []byte(fmt.Sprintf("Got message: %s", d.Body))
}

func handleClientUsage(ctx context.Context, d *amqp.Delivery) []byte {
	logger.Infof("Handling 'Client usage' request")

	cert := connection.Certificates{
		Cert: "client/cert.pem",
		Key:  "client/key.pem",
		CA:   "client/cacert.pem",
	}

	c := client.New("amqps://guest:guest@localhost:5671/", cert)

	request := client.NewRequest("hello_world").WithStringBody("Sent with client")
	response, err := c.Send(request)
	if err != nil {
		logger.Warnf("Something went wrong: %s", err)
		return []byte(err.Error())
	}

	return []byte(fmt.Sprintf("Response from next endpoint: %s", response.Body))
}
