package amqprpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bombsimon/amqp-rpc/testhelpers"
	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

var serverTestURL = "amqp://guest:guest@localhost:5672/"

func TestSendWithReply(t *testing.T) {
	cert := Certificates{}

	s := NewServer(serverTestURL).WithDialConfig(amqp.Config{
		TLSClientConfig: cert.TLSConfig(),
	})

	NotEqual(t, s.dialconfig.TLSClientConfig, nil)

	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := testhelpers.StartServer(s)
	defer stop()

	c := NewClient(serverTestURL)
	request := NewRequest("myqueue").WithBody("this is a message")
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

	s := NewServer(serverTestURL).AddMiddleware(mw)

	s.Bind(DirectBinding("allowed", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprint(rw, "this is allowed")
	}))

	s.Bind(DirectBinding("denied", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprint(rw, "this is not allowed")
	}))

	stop := testhelpers.StartServer(s)
	defer stop()

	c := NewClient(serverTestURL)

	request := NewRequest("allowed")
	reply, err := c.Send(request)

	Equal(t, err, nil)
	Equal(t, reply.Body, []byte("this is allowed"))

	request = NewRequest("denied")
	reply, err = c.Send(request)

	Equal(t, err, nil)
	Equal(t, reply.Body, []byte("routing key 'denied' is not allowed"))
}

func TestServerReconnect(t *testing.T) {
	conn, err := amqp.Dial(serverTestURL)

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
	s := NewServer(serverTestURL).WithDialConfig(amqp.Config{Dial: dialer})

	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		time.Sleep(500 * time.Millisecond)
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := testhelpers.StartServer(s)
	defer stop()
	c := NewClient(serverTestURL)

	for i := 0; i < 2; i++ {
		message := fmt.Sprintf("this is message %v", i)
		request := NewRequest("myqueue").WithBody(message)
		reply, err := c.Send(request)
		Equal(t, err, nil)

		conn := <-connections
		conn.Close()

		Equal(t, reply.Body, []byte(fmt.Sprintf("Got message: %s", message)))
	}
}
