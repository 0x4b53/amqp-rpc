package amqprpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

var serverTestURL = "amqp://guest:guest@localhost:5672/"

func TestSendWithReply(t *testing.T) {
	cert := Certificates{}

	s := NewServer(serverTestURL).WithDialConfig(amqp.Config{
		TLSClientConfig: cert.TLSConfig(),
	})

	assert.NotNil(t, s.dialconfig.TLSClientConfig, "dialconfig on server is set")

	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := startAndWait(s)
	defer stop()

	c := NewClient(serverTestURL)
	request := NewRequest().WithRoutingKey("myqueue").WithBody("this is a message")
	reply, err := c.Send(request)

	assert.Nil(t, err, "client exist")
	assert.Equal(t, []byte("Got message: this is a message"), reply.Body, "got reply")
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

	request := NewRequest().WithRoutingKey("allowed")
	reply, err := c.Send(request)

	assert.Nil(t, err, "no error")
	assert.Equal(t, []byte("this is allowed"), reply.Body, "allowed middleware callable")

	request = NewRequest().WithRoutingKey("denied")
	reply, err = c.Send(request)

	assert.Nil(t, err, "no error")
	assert.Equal(t, []byte("routing key 'denied' is not allowed"), reply.Body, "denied middleware not callable")
}

func TestServerReconnect(t *testing.T) {
	dialer, connections := testDialer()
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
		request := NewRequest().WithRoutingKey("myqueue").WithBody(message)
		reply, err := c.Send(request)
		assert.Nil(t, err, "no error")

		conn := <-connections
		conn.Close()

		assert.Equal(t, []byte(fmt.Sprintf("Got message: %s", message)), reply.Body, "message received after reconnect")
	}
}

func TestServerOnStarted(t *testing.T) {
	errs := make(chan string, 4)

	s := NewServer(serverTestURL)
	s.OnStarted(func(inC, outC *amqp.Connection, inCh, outCh *amqp.Channel) {
		if inC == nil {
			errs <- "inC was nil"
		}
		if outC == nil {
			errs <- "outC was nil"
		}
		if inCh == nil {
			errs <- "inCh was nil"
		}
		if outCh == nil {
			errs <- "outCh was nil"
		}

		close(errs)
	})

	stop := startAndWait(s)
	defer stop()

	select {
	case e, ok := <-errs:
		if ok {
			t.Fatal(e)
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
		assert.Nil(t, nil, "no error")
	case <-time.After(10 * time.Second):
		// No success within 10 seconds
		t.Error("Didn't succeed to close server")
	}
}
