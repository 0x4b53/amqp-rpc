package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bombsimon/amqp-rpc/client"
	"github.com/bombsimon/amqp-rpc/connection"
	"github.com/bombsimon/amqp-rpc/testhelpers"
	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestSendWithReply(t *testing.T) {
	cert := connection.Certificates{}

	s := New(url).WithDialConfig(amqp.Config{
		TLSClientConfig: cert.TLSConfig(),
	})

	NotEqual(t, s.dialconfig.TLSClientConfig, nil)

	s.AddHandler("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	})

	startServer(s)

	c := client.New(url)
	request := client.NewRequest("myqueue").WithStringBody("this is a message")
	reply, err := c.Send(request)

	Equal(t, err, nil)
	Equal(t, reply.Body, []byte("Got message: this is a message"))
}

func TestMiddleware(t *testing.T) {
	mw := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
			if ctx.Value(CtxQueueName).(string) == "denied" {
				fmt.Fprint(rw, "routing key 'denied' is not allowed")
				return
			}

			next(ctx, rw, d)
		}
	}

	s := New(url).AddMiddleware(mw)

	s.AddHandler("allowed", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprint(rw, "this is allowed")
	})

	s.AddHandler("denied", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprint(rw, "this is not allowed")
	})

	startServer(s)

	c := client.New(url)

	request := client.NewRequest("allowed")
	reply, err := c.Send(request)

	Equal(t, err, nil)
	Equal(t, reply.Body, []byte("this is allowed"))

	request = client.NewRequest("denied")
	reply, err = c.Send(request)

	Equal(t, err, nil)
	Equal(t, reply.Body, []byte("routing key 'denied' is not allowed"))
}

func TestReconnect(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	Equal(t, err, nil)
	defer conn.Close()

	ch, err := conn.Channel()
	Equal(t, err, nil)
	defer ch.Close()

	ch.QueueDelete(
		"myqueue",
		false, // ifUnused
		false, // ifEmpty
		false, // noWait
	)

	dialer, connections := testhelpers.TestDialer(t)
	s := New(url).WithDialConfig(amqp.Config{Dial: dialer})

	s.AddHandler("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		time.Sleep(500 * time.Millisecond)
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	})

	startServer(s)
	c := client.New(url)

	for i := 0; i < 2; i++ {
		message := []byte(fmt.Sprintf("this is message %v", i))
		request := client.NewRequest("myqueue").WithBody(message)
		reply, err := c.Send(request)
		Equal(t, err, nil)

		conn := <-connections
		conn.Close()

		Equal(t, reply.Body, []byte(fmt.Sprintf("Got message: %s", message)))
	}
}

func TestFanout(t *testing.T) {
	var timesCalled = 0

	s1 := New(url)
	s2 := New(url)
	s3 := New(url)

	fanoutHandler := func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Println("HERE?")
		timesCalled++
	}

	s1.AddFanoutHandler("fanout-exchange", fanoutHandler)
	s2.AddFanoutHandler("fanout-exchange", fanoutHandler)
	s3.AddFanoutHandler("fanout-exchange", fanoutHandler)

	startServer(s1)
	startServer(s2)
	startServer(s3)

	c := client.New(url)
	_, err := c.Send(client.NewRequest("").WithExchange("fanout-exchange"))
	time.Sleep(200 * time.Millisecond)

	Equal(t, err, nil)
	Equal(t, timesCalled, 3)
}

func startServer(s *RPCServer) {
	go s.ListenAndServe()
	time.Sleep(50 * time.Millisecond)
}
