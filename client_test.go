package amqprpc

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

const (
	clientTestURL = "amqp://guest:guest@localhost:5672/"
)

func TestClient(t *testing.T) {
	s := NewServer(clientTestURL)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := startAndWait(s)
	defer stop()

	client := NewClient("amqp://guest:guest@localhost:5672/")
	defer client.Stop()

	assert.NotNil(t, client, "client exist")

	request := NewRequest().WithRoutingKey("myqueue").WithBody("client testing")
	response, err := client.Send(request)
	assert.Nil(t, err, "no errors from sending")
	assert.Equal(t, []byte("Got message: client testing"), response.Body, "correct body in response")
}

func TestClientConfig(t *testing.T) {
	cert := Certificates{}
	certClient := NewClient(clientTestURL).WithTLS(cert.TLSConfig())
	defer certClient.Stop()

	assert.NotNil(t, certClient, "client with certificate exist")

	ac := amqp.Config{}
	qdSettings := QueueDeclareSettings{}
	cSettings := ConsumeSettings{}

	acClient := NewClient(clientTestURL).WithDialConfig(ac).
		WithQueueDeclareSettings(qdSettings).
		WithConsumeSettings(cSettings).
		WithTimeout(2500 * time.Millisecond)
	defer acClient.Stop()

	assert.NotNil(t, acClient, "configured client exist")
}

func TestClientReconnect(t *testing.T) {
	client := NewClient(clientTestURL)
	defer client.Stop()

	assert.NotNil(t, client, "client with dialer exist")

	// Force a connection by calling send.
	_, err := client.Send(NewRequest().WithResponse(false))
	assert.NoError(t, err)

	closeAllConnections()

	_, err = client.Send(NewRequest().WithResponse(false))
	assert.NoError(t, err)
}

func TestClientTimeout(t *testing.T) {
	s := NewServer(clientTestURL)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		expiration, _ := strconv.Atoi(d.Expiration)

		if d.Headers["timeout"].(bool) {
			assert.NotEqual(t, 0, expiration)
		} else {
			assert.Equal(t, 0, expiration)
		}

		time.Sleep(2 * time.Millisecond)
	}))

	stop := startAndWait(s)
	defer stop()

	cases := []struct {
		name        string
		client      *Client
		request     *Request
		wantTimeout bool
	}{
		{
			name:        "Client with timeout but no timeout on the Request",
			client:      NewClient(clientTestURL).WithTimeout(1 * time.Millisecond),
			request:     NewRequest(),
			wantTimeout: true,
		},
		{
			name:        "Request with timeout but no timeout on the Client",
			client:      NewClient(clientTestURL),
			request:     NewRequest().WithTimeout(1 * time.Millisecond),
			wantTimeout: true,
		},
		{
			name:        "Request timeout overrides the Client timeout",
			client:      NewClient(clientTestURL).WithTimeout(10 * time.Second),
			request:     NewRequest().WithTimeout(1 * time.Millisecond),
			wantTimeout: true,
		},
		{
			name:        "Request without reply has no timeout",
			client:      NewClient(clientTestURL).WithTimeout(10 * time.Second),
			request:     NewRequest().WithResponse(false),
			wantTimeout: false,
		},
	}

	for _, tc := range cases {
		tc.request.WriteHeader("timeout", tc.wantTimeout)
		tc.request.WithRoutingKey("myqueue")

		t.Run(tc.name, func(t *testing.T) {
			defer tc.client.Stop()

			response, err := tc.client.Send(tc.request)
			if !tc.wantTimeout {
				assert.Nil(t, err)
				return
			}

			assert.Equal(t, ErrTimeout, err)
			assert.Nil(t, response)
		})
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
	defer c.Stop()

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

func TestClient_OnStarted(t *testing.T) {
	s := NewServer(clientTestURL)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := startAndWait(s)
	defer stop()

	errs := make(chan string, 4)

	c := NewClient(clientTestURL)
	defer c.Stop()

	c.OnStarted(func(inC, outC *amqp.Connection, inCh, outCh *amqp.Channel) {
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

	// Since the client is lazy, OnStarted isn't called until the first .Send().
	request := NewRequest().
		WithRoutingKey("myqueue").
		WithBody("client testing").
		WithResponse(false)

	_, err := c.Send(request)
	assert.Nil(t, err)

	select {
	case e, ok := <-errs:
		if ok {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Error("OnStarted was never called")
	}
}
