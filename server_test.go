package amqprpc

import (
	"context"
	"fmt"
	"testing"
	"time"

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

	stop := startAndWait(s)
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

	stop := startAndWait(s)
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
	dialer, connections := testDialer(t)
	s := NewServer(serverTestURL).WithDialConfig(amqp.Config{Dial: dialer})

	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		time.Sleep(10 * time.Millisecond)
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := startAndWait(s)
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

func TestServerOnStarted(t *testing.T) {
	didStart := make(chan struct{})
	notNills := [4]interface{}{}

	s := NewServer(serverTestURL)
	s.OnStarted(func(inC, outC *amqp.Connection, inCh, outCh *amqp.Channel) {
		notNills[0] = inC
		notNills[1] = outC
		notNills[2] = inCh
		notNills[3] = outCh
		close(didStart)
	})

	stop := startAndWait(s)
	defer stop()

	select {
	case <-didStart:
		for i := range notNills {
			NotEqual(t, notNills[i], nil)
		}
	case <-time.After(time.Second):
		t.Error("OnStarted was never called")
	}
}

func TestStopWhenStarting(t *testing.T) {
	s := NewServer("amqp://guest:guest@wont-connect.com:5672")

	done := make(chan struct{})
	go func() {
		s.ListenAndServe()
		close(done)
	}()

	// Cannot use OnStarted() since we won't successfully start.
	time.Sleep(10 * time.Millisecond)
	s.Stop()

	// Block so we're sure that we actually exited.
	select {
	case <-done:
		// The done channel was closed!
		Equal(t, nil, nil)
	case <-time.After(10 * time.Second):
		// No success within 10 seconds
		t.Error("Didn't succeed to close server")
	}
}
