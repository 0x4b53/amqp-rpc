package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/bombsimon/amqp-rpc/client"
	"github.com/bombsimon/amqp-rpc/logger"
	"github.com/bombsimon/amqp-rpc/server"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

var (
	randomHeader string
	url          = "amqp://guest:guest@localhost:5672"
)

func main() {
	go startServer()

	l := log.New(ioutil.Discard, "", 0)
	logger.SetInfoLogger(l)
	logger.SetWarnLogger(l)

	c := client.New(url).
		AddPreSendMiddleware(setPassword).
		AddPostSendMiddleware(exchangePassword)

	r := client.NewRequest("exchanger")

	for _, i := range []int{1, 2, 3} {
		fmt.Printf("%-10s %d: randomHeader is '%s'\n", "Request", i, randomHeader)

		resp, err := c.Send(r)
		if err != nil {
			fmt.Println("Woops: ", err)
		} else {
			fmt.Printf("%-10s %d: randomHeader is '%s' (body is '%s')\n", "Response", i, resp.Headers["randomHeader"], resp.Body)
		}
	}
}

// Middleware executing before send in client.
func setPassword(next client.PreSendFunc) client.PreSendFunc {
	// This coud simulate the usage of i.e. a JWT that needs to be passed in
	// each requests and exchange at the server.
	if randomHeader == "" {
		fmt.Println(">> I'm being executed before Send(), I'm ensuring you've got a randomHeader!")
		randomHeader = uuid.Must(uuid.NewV4()).String()
	}

	return func(r *client.Request) {
		r.Headers["randomHeader"] = randomHeader

		next(r)
	}
}

// Middleware executing after send in client.
func exchangePassword(next client.PostSendFunc) client.PostSendFunc {
	return func(d *amqp.Delivery, e error) {
		if newHeader, ok := d.Headers["randomHeader"].(string); ok {
			randomHeader = newHeader
		}

		next(d, e)
	}
}

//Middleware executing before or after being handled in server.
func exchangeHeader(next server.HandlerFunc) server.HandlerFunc {
	return func(ctx context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
		next(ctx, rw, d)

		rw.WriteHeader("randomHeader", uuid.Must(uuid.NewV4()).String())
	}
}

func startServer() {
	s := server.New(url)

	s.AddMiddleware(exchangeHeader)

	s.Bind(server.DirectBinding("exchanger", func(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, d.Headers["randomHeader"].(string))
	}))

	s.ListenAndServe()
}
