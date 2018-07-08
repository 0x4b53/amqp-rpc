package main

import (
	"context"
	"fmt"
	"time"

	"github.com/bombsimon/amqp-rpc/client"
	"github.com/bombsimon/amqp-rpc/server"

	"github.com/streadway/amqp"
)

var timesCalled = 0

func main() {
	s1 := server.New("amqp://guest:guest@localhost:5672/")
	s2 := server.New("amqp://guest:guest@localhost:5672/")
	s3 := server.New("amqp://guest:guest@localhost:5672/")

	// No need for three handlers but it's just to show that different methods
	// will be called.
	s1.AddFanoutHandler("cool-exchange", fanoutHandlerOne)
	s2.AddFanoutHandler("cool-exchange", fanoutHandlerTwo)
	s3.AddFanoutHandler("cool-exchange", fanoutHandlerThree)

	s1.AddHandler("times_called", timesCalledHandler)

	go s1.ListenAndServe()
	go s2.ListenAndServe()
	go s3.ListenAndServe()

	c := client.New("amqp://guest:guest@localhost:5672/")
	r := client.NewRequest("").
		WithExchange("cool-exchange").
		WithStringBody("Seding fanout").
		WithResponse(false)

	_, err := c.Send(r)
	if err != nil {
		fmt.Println("Woops: ", err)
	}

	time.Sleep(100 * time.Millisecond)

	response, err := c.Send(client.NewRequest("times_called"))
	if err != nil {
		fmt.Println("Woops: ", err)
	}

	fmt.Printf("The fanout call has been handled %s times\n", response.Body)
}

func fanoutHandlerOne(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	timesCalled++
	fmt.Fprint(rw, "First server handled request")
}

func fanoutHandlerTwo(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	timesCalled++
	fmt.Fprint(rw, "Second server handled request")
}

func fanoutHandlerThree(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	timesCalled++
	fmt.Fprint(rw, "Third server handled request")
}

func timesCalledHandler(c context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
	fmt.Fprint(rw, timesCalled)
}
