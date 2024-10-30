package main

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	amqprpc "github.com/0x4b53/amqp-rpc/v5"
)

// nolint:gochecknoglobals
var timesCalled = 0

func main() {
	s1 := amqprpc.NewServer("amqp://guest:guest@localhost:5672/")
	s2 := amqprpc.NewServer("amqp://guest:guest@localhost:5672/")
	s3 := amqprpc.NewServer("amqp://guest:guest@localhost:5672/")

	// No need for three handlers but it's just to show that different methods
	// will be called.
	s1.Bind(amqprpc.FanoutBinding("cool-exchange", fanoutHandlerOne))
	s2.Bind(amqprpc.FanoutBinding("cool-exchange", fanoutHandlerTwo))
	s3.Bind(amqprpc.FanoutBinding("cool-exchange", fanoutHandlerThree))

	s1.Bind(amqprpc.DirectBinding("times_called", timesCalledHandler))

	go s1.ListenAndServe()

	go s2.ListenAndServe()

	go s3.ListenAndServe()

	time.Sleep(1 * time.Second)

	c := amqprpc.NewClient("amqp://guest:guest@localhost:5672/")
	r := amqprpc.NewRequest().
		WithExchange("cool-exchange").
		WithBody("Seding fanout").
		WithResponse(false)

	_, err := c.Send(r)
	if err != nil {
		fmt.Println("Woops: ", err)
	}

	time.Sleep(1 * time.Second)

	response, err := c.Send(amqprpc.NewRequest().WithRoutingKey("times_called"))
	if err != nil {
		fmt.Println("Woops: ", err)
	}

	fmt.Printf("The fanout call has been handled %s times\n", response.Body)
}

func fanoutHandlerOne(c context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	timesCalled++
	fmt.Fprint(rw, "First server handled request")
}

func fanoutHandlerTwo(c context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	timesCalled++
	fmt.Fprint(rw, "Second server handled request")
}

func fanoutHandlerThree(c context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	timesCalled++
	fmt.Fprint(rw, "Third server handled request")
}

func timesCalledHandler(c context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
	fmt.Fprint(rw, timesCalled)
}
