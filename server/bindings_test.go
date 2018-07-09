package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bombsimon/amqp-rpc/client"
	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

func TestFanout(t *testing.T) {
	var timesCalled = 0

	s1 := New(url)
	s2 := New(url)
	s3 := New(url)

	fanoutHandler := func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		timesCalled++
	}

	s1.Bind(FanoutBinding("fanout-exchange", fanoutHandler))
	s1.Bind(FanoutBinding("fanout-exchange", fanoutHandler))
	s1.Bind(FanoutBinding("fanout-exchange", fanoutHandler))

	startServer(s1)
	startServer(s2)
	startServer(s3)

	// Ensure all queues are declared and ready.
	time.Sleep(1 * time.Second)

	c := client.New(url)
	_, err := c.Send(client.NewRequest("").WithExchange("fanout-exchange").WithResponse(false))

	// Ensure all handlers have added to the timesCalled variable.
	time.Sleep(1 * time.Second)

	Equal(t, err, nil)
	Equal(t, timesCalled, 3)
}

func TestTopic(t *testing.T) {
	var wasCalled = make(chan bool)

	s := New(url)
	c := client.New(url)

	handler := func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		wasCalled <- true
	}

	handlerNotMatched := func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		// We should never fail because this should not match
		t.Fail()
	}

	s.Bind(TopicBinding("somewhere.#", handler))
	s.Bind(TopicBinding("somewhere.*", handler))
	s.Bind(TopicBinding("not_here.*", handlerNotMatched))

	startServer(s)

	// Ensure 'somewhere.*' matches 'somewhere.there'.
	_, err := c.Send(client.NewRequest("somewhere.there").WithExchange("amq.topic").WithResponse(false))
	Equal(t, err, nil)

	called, ok := <-wasCalled

	Equal(t, ok, true)
	Equal(t, called, true)

	// Ensure 'somewhere.#' matches 'somewhere.over.there'.
	_, err = c.Send(client.NewRequest("somewhere.over.there").WithExchange("amq.topic").WithResponse(false))
	Equal(t, err, nil)

	called, ok = <-wasCalled

	Equal(t, ok, true)
	Equal(t, called, true)

	// Ensure 'not_here.*' does NOT match 'not_here.or.there'.
	_, err = c.Send(client.NewRequest("not_here.or.there").WithExchange("amq.topic").WithResponse(false))
	Equal(t, err, nil)

	// Ensure we don't exit before potential failures.
	time.Sleep(100 * time.Millisecond)
}

func TestHeaders(t *testing.T) {
	s := New(url)
	c := client.New(url)

	handler := func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Hello, world")
	}

	h := amqp.Table{
		"x-match": "all",
		"foo":     "bar",
	}

	s.Bind(HeadersBinding(h, handler))

	startServer(s)

	// Ensure 'somewhere.*' matches 'somewhere.there'.
	response, err := c.Send(client.NewRequest("").WithExchange("amq.headers").WithHeaders(amqp.Table{"foo": "bar"}))

	Equal(t, err, nil)
	Equal(t, response.Body, []byte("Hello, world"))
}
