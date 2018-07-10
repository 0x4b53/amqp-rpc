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
	s2.Bind(FanoutBinding("fanout-exchange", fanoutHandler))
	s3.Bind(FanoutBinding("fanout-exchange", fanoutHandler))

	stop1 := startServer(s1)
	stop2 := startServer(s2)
	stop3 := startServer(s3)
	defer stop1()
	defer stop2()
	defer stop3()

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
	wasCalled := map[string]chan string{
		"foo.#": make(chan string),
		"foo.*": make(chan string),
		"baz.*": make(chan string),
	}

	s := New(url)
	c := client.New(url)

	s.Bind(TopicBinding("foo.#", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		wasCalled["foo.#"] <- string(d.Body)
	}))
	s.Bind(TopicBinding("foo.*", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		wasCalled["foo.*"] <- string(d.Body)
	}))
	s.Bind(TopicBinding("baz.*", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		wasCalled["baz.*"] <- string(d.Body)
	}))

	stop := startServer(s)
	defer stop()

	cases := []struct {
		request string
		called  map[string]bool
	}{
		{
			request: "foo.bar",
			called:  map[string]bool{"foo.#": true, "foo.*": true, "baz.*": false},
		},
		{
			request: "foo.bar.baz",
			called:  map[string]bool{"foo.#": true, "foo.*": false, "baz.*": false},
		},
		{
			request: "baz.bar.foo",
			called:  map[string]bool{"foo.#": false, "foo.*": false, "baz.*": false},
		},
	}

	for _, tc := range cases {
		t.Run(tc.request, func(t *testing.T) {
			_, err := c.Send(client.NewRequest(tc.request).WithStringBody(tc.request).WithExchange("amq.topic").WithResponse(false))
			Equal(t, err, nil)

			for key, expectCalled := range tc.called {
				select {
				case body := <-wasCalled[key]:
					if expectCalled != true {
						t.Errorf("%s WAS called on %s with body %s", key, tc.request, body)
					}
					Equal(t, body, tc.request)
				case <-time.After(10 * time.Millisecond):
					if expectCalled == true {
						t.Errorf("%s NOT called on %s", key, tc.request)
					}
				}
			}
		})
	}
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

	stop := startServer(s)
	defer stop()

	// Ensure 'somewhere.*' matches 'somewhere.there'.
	response, err := c.Send(client.NewRequest("").WithExchange("amq.headers").WithHeaders(amqp.Table{"foo": "bar"}))

	Equal(t, err, nil)
	Equal(t, response.Body, []byte("Hello, world"))
}
