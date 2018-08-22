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

var clientTestURL = "amqp://guest:guest@localhost:5672/"

func TestClient(t *testing.T) {
	s := NewServer(clientTestURL)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := testhelpers.StartServer(s)
	defer stop()

	client := NewClient("amqp://guest:guest@localhost:5672/")
	NotEqual(t, client, nil)

	request := NewRequest("myqueue").WithBody("client testing")
	response, err := client.Send(request)
	Equal(t, err, nil)
	Equal(t, response.Body, []byte("Got message: client testing"))
}

func TestClientConfig(t *testing.T) {
	cert := Certificates{}
	certClient := NewClient(clientTestURL).WithTLS(cert)

	NotEqual(t, certClient, nil)

	ac := amqp.Config{}
	qdSettings := QueueDeclareSettings{}
	cSettings := ConsumeSettings{}

	acClient := NewClient(clientTestURL).WithDialConfig(ac).
		WithQueueDeclareSettings(qdSettings).
		WithConsumeSettings(cSettings).
		WithTimeout(2500 * time.Millisecond)

	NotEqual(t, acClient, nil)
}

func TestReconnect(t *testing.T) {
	dialer, connections := testhelpers.TestDialer(t)
	client := NewClient(clientTestURL).WithDialConfig(amqp.Config{Dial: dialer})
	NotEqual(t, client, nil)

	// Force a connection by calling send.
	_, err := client.Send(NewRequest("myqueue").WithResponse(false))
	Equal(t, err, nil)

	// Hook into the connection, disconnect
	time.Sleep(100 * time.Millisecond)
	conn, _ := <-connections
	conn.Close()

	_, err = client.Send(NewRequest("myqueue").WithBody("client testing"))
	NotEqual(t, err, nil)

	// Ensure we're reconnected
	time.Sleep(100 * time.Millisecond)

	_, err = client.Send(NewRequest("myqueue").WithBody("client testing").WithResponse(false))
	Equal(t, err != nil, false)
}

func TestTimeout(t *testing.T) {
	s := NewServer(clientTestURL)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		time.Sleep(1 * time.Millisecond)
	}))

	stop := testhelpers.StartServer(s)
	defer stop()

	cases := []struct {
		client  *Client
		request *Request
	}{
		// Client with timeout but no timeout on the Request.
		{
			client:  NewClient(clientTestURL).WithTimeout(1 * time.Millisecond),
			request: NewRequest("myqueue"),
		},
		// Request with timeout but no timeout on the Client.
		{
			client:  NewClient(clientTestURL),
			request: NewRequest("myqueue").WithTimeout(1 * time.Millisecond),
		},
		// Request timeout overrides the Client timeout.
		{
			client:  NewClient(clientTestURL).WithTimeout(10 * time.Second),
			request: NewRequest("myqueue").WithTimeout(1 * time.Millisecond),
		},
	}

	for _, tc := range cases {
		response, err := tc.client.Send(tc.request)
		Equal(t, err, ErrTimeout)
		Equal(t, response, nil)
	}
}

func TestGracefulShutdown(t *testing.T) {
	s := NewServer(clientTestURL)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "hello")
	}))

	stop := testhelpers.StartServer(s)
	defer stop()

	c := NewClient(clientTestURL)

	r, err := c.Send(NewRequest("myqueue"))

	Equal(t, err, nil)
	Equal(t, string(r.Body), "hello")

	c.Stop()

	// We should have a new synx.Once and reconnect after a disconnect.
	time.Sleep(50 * time.Millisecond)

	r, err = c.Send(NewRequest("myqueue"))
	Equal(t, err, nil)
	Equal(t, string(r.Body), "hello")
}
