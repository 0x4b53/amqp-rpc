package amqprpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

var clientTestURL = "amqp://guest:guest@localhost:5672/"

func TestClient(t *testing.T) {
	s := NewServer(clientTestURL)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := startAndWait(s)
	defer stop()

	client := NewClient("amqp://guest:guest@localhost:5672/")
	assert.NotNil(t, client, "client exist")

	request := NewRequest().WithRoutingKey("myqueue").WithBody("client testing")
	response, err := client.Send(request)
	assert.Nil(t, err, "no errors from sending")
	assert.Equal(t, []byte("Got message: client testing"), response.Body, "correct body in response")
}

func TestClientConfig(t *testing.T) {
	cert := Certificates{}
	certClient := NewClient(clientTestURL).WithTLS(cert)

	assert.NotNil(t, certClient, "client with certificate exist")

	ac := amqp.Config{}
	qdSettings := QueueDeclareSettings{}
	cSettings := ConsumeSettings{}

	acClient := NewClient(clientTestURL).WithDialConfig(ac).
		WithQueueDeclareSettings(qdSettings).
		WithConsumeSettings(cSettings).
		WithTimeout(2500 * time.Millisecond)

	assert.NotNil(t, acClient, "configured client exist")
}

func TestClientReconnect(t *testing.T) {
	dialer, connections := testDialer()
	client := NewClient(clientTestURL).WithDialConfig(amqp.Config{Dial: dialer})
	assert.NotNil(t, client, "client with dialer exist")

	// Force a connection by calling send.
	_, err := client.Send(NewRequest().WithResponse(false))
	assert.Nil(t, err, "no error from send without response")

	// Hook into the connection, disconnect
	conn := <-connections
	conn.Close()
	time.Sleep(10 * time.Millisecond)

	r := NewRequest().WithBody("client testing").WithResponse(false)
	r.numRetries = 100

	_, err = client.Send(r)
	assert.Contains(t, err.Error(), "channel/connection is not open", "disconnected client yields error")

	// Ensure we're reconnected
	time.Sleep(100 * time.Millisecond)

	_, err = client.Send(NewRequest().WithBody("client testing").WithResponse(false))
	assert.Nil(t, err, "retry after reconnect successful")
}

func TestClientTimeout(t *testing.T) {
	s := NewServer(clientTestURL)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		time.Sleep(2 * time.Millisecond)
	}))

	stop := startAndWait(s)
	defer stop()

	cases := []struct {
		client  *Client
		request *Request
	}{
		// Client with timeout but no timeout on the Request.
		{
			client:  NewClient(clientTestURL).WithTimeout(1 * time.Millisecond),
			request: NewRequest().WithRoutingKey("myqueue"),
		},
		// Request with timeout but no timeout on the Client.
		{
			client:  NewClient(clientTestURL),
			request: NewRequest().WithRoutingKey("myqueue").WithTimeout(1 * time.Millisecond),
		},
		// Request timeout overrides the Client timeout.
		{
			client:  NewClient(clientTestURL).WithTimeout(10 * time.Second),
			request: NewRequest().WithRoutingKey("myqueue").WithTimeout(1 * time.Millisecond),
		},
	}

	for _, tc := range cases {
		response, err := tc.client.Send(tc.request)
		assert.Equal(t, ErrTimeout, err, "error indicates timeout")
		assert.Nil(t, response, "no response given")
	}
}

func TestGracefulShutdown(t *testing.T) {
	s := NewServer(clientTestURL)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "hello")
	}))

	stop := startAndWait(s)
	defer stop()

	c := NewClient(clientTestURL)

	r, err := c.Send(NewRequest().WithRoutingKey("myqueue"))

	assert.Nil(t, err, "no error before shutting down")
	assert.Equal(t, "hello", string(r.Body), "correct body")

	c.Stop()

	// We should have a new synx.Once and reconnect after a disconnect.
	time.Sleep(50 * time.Millisecond)

	r, err = c.Send(NewRequest().WithRoutingKey("myqueue"))
	assert.Nil(t, err, "no error when sending after stop")
	assert.Equal(t, "hello", string(r.Body), "correct body after sending after stop")
}
