package amqprpc

import (
	"context"
	"fmt"
	"strings"
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
	request := NewRequest("myqueue").WithStringBody("this is a message")
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
		message := []byte(fmt.Sprintf("this is message %v", i))
		request := NewRequest("myqueue").WithBody(message)
		reply, err := c.Send(request)
		Equal(t, err, nil)

		conn := <-connections
		conn.Close()

		Equal(t, reply.Body, []byte(fmt.Sprintf("Got message: %s", message)))
	}
}

func TestServerPanicRecovery(t *testing.T) {
	calls := map[string]int{
		"crash":       0,
		"crash-retry": 0,
	}

	panicHandler := func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		key := ctx.Value(CtxQueueName).(string)
		calls[key]++

		panic("not your best friend")
	}

	s1 := NewServer(serverTestURL).WithMaxRetries(0)
	s2 := NewServer(serverTestURL).WithMaxRetries(2)

	s1.Bind(DirectBinding("crash", panicHandler))
	s2.Bind(DirectBinding("crash-retry", panicHandler))

	s1Stop := testhelpers.StartServer(s1)
	s2Stop := testhelpers.StartServer(s2)
	defer s1Stop()
	defer s2Stop()

	c := NewClient(serverTestURL)

	// XXX: First server should not retry at all
	r := NewRequest("crash")
	reply, err := c.Send(r)

	NotEqual(t, err, nil)
	Equal(t, err, ErrServerCrashed)

	NotEqual(t, reply, nil)
	Equal(t, strings.HasPrefix(string(reply.Body), "crashed when running handler"), true)

	// No retries was made when using s1
	Equal(t, calls["crash"], 1)

	// XXX: Second server should retry two times.
	r = NewRequest("crash-retry")
	reply, err = c.Send(r)

	NotEqual(t, err, nil)
	NotEqual(t, reply, nil)

	// More than one new tries should exist when using s2
	Equal(t, calls["crash-retry"] >= 2, true)

	// XXX: Third server should result in a timeout since we retry so many
	// times we're not there to listen to the final anwser.
	r = NewRequest("crash-retry").WithTimeout(2 * time.Second)
	reply, err = c.Send(r)

	NotEqual(t, err, nil)
	Equal(t, err, ErrTimeout)
	Equal(t, reply, nil)
}
