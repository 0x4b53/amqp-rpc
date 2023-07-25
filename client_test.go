package amqprpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	_, client, start, stop := initTest()
	defer stop()

	start()

	request := NewRequest().WithRoutingKey(defaultTestQueue).WithBody("client testing")
	response, err := client.Send(request)

	require.NoError(t, err)
	assert.Equal(t, []byte("Got message: client testing"), response.Body, "correct body in response")
}

func TestClientNoConfirmMode(t *testing.T) {
	_, client, start, stop := initTest()
	defer stop()

	client.WithConfirmMode(false)

	start()

	request := NewRequest().WithRoutingKey(defaultTestQueue).WithBody("client testing")
	response, err := client.Send(request)

	require.NoError(t, err)
	assert.Equal(t, []byte("Got message: client testing"), response.Body, "correct body in response")
}

func TestClientDataRace(_ *testing.T) {
	_, client, start, stop := initTest()
	defer stop()

	start()

	wg := sync.WaitGroup{}

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)

		go func() {
			_, _ = client.Send(
				NewRequest().
					WithRoutingKey(defaultTestQueue).
					WithBody("client testing"),
			)

			wg.Done()
		}()
	}

	wg.Wait()
}

func TestClientReturn(t *testing.T) {
	_, client, start, stop := initTest()
	defer stop()

	start()

	tests := []struct {
		name         string
		withResponse bool
	}{
		{
			name:         "WithResponse: true",
			withResponse: true,
		},
		{
			name:         "WithResponse: false",
			withResponse: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := NewRequest().
				WithResponse(tt.withResponse).
				WithRoutingKey("not-exists")

			_, err := client.Send(request)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrRequestReturned))
			assert.Contains(t, err.Error(), "NO_ROUTE")
		})
	}
}

func TestClient_ConfirmsConsumer_return(t *testing.T) {
	client := NewClient("")
	client.requestsMap = RequestMap{
		byDeliveryTag:   make(map[uint64]*Request),
		byCorrelationID: make(map[string]*Request),
	}

	returns := make(chan amqp.Return)
	confirms := make(chan amqp.Confirmation)

	go client.runConfirmsConsumer(confirms, returns)

	tests := []struct {
		name       string
		isUnknown  bool
		wantReturn bool
		wantErr    bool
	}{
		{
			name:       "known return sets error",
			isUnknown:  false,
			wantReturn: true,
			wantErr:    true,
		},
		{
			name:       "unknown does nothing",
			isUnknown:  true,
			wantReturn: false,
			wantErr:    false,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := NewRequest().
				WithResponse(false)

			request.confirmed = make(chan struct{})
			request.response = make(chan *amqp.Delivery)
			request.errChan = make(chan error)
			request.deliveryTag = uint64(i)
			request.Publishing.CorrelationId = tt.name

			if !tt.isUnknown {
				client.requestsMap.Set(request)
			}

			go func() {
				returns <- amqp.Return{
					CorrelationId: request.Publishing.CorrelationId,
				}

				confirms <- amqp.Confirmation{
					DeliveryTag: request.deliveryTag,
					Ack:         true,
				}
			}()

			select {
			case <-request.confirmed:
				assert.True(t, tt.wantErr, "request confirmed")
			case <-time.After(10 * time.Millisecond):
				assert.False(t, tt.wantErr, "request not confirmed")
			}

			select {
			case <-request.errChan:
				assert.True(t, tt.wantErr, "got error")
			case <-time.After(10 * time.Millisecond):
				assert.False(t, tt.wantErr, "no error")
			}
		})
	}
}

func TestClient_ConfirmsConsumer_confirm(t *testing.T) {
	client := NewClient("")
	client.requestsMap = RequestMap{
		byDeliveryTag:   make(map[uint64]*Request),
		byCorrelationID: make(map[string]*Request),
	}

	confirms := make(chan amqp.Confirmation)

	go client.runConfirmsConsumer(confirms, make(chan amqp.Return))

	tests := []struct {
		name            string
		ack             bool
		wantNoConfirm   bool
		wantNilResponse bool
		wantErr         bool
		isReturned      bool
		isUnknown       bool
	}{
		{
			name:          "unknown request does nothing",
			wantNoConfirm: true,
			isUnknown:     true,
		},
		{
			name:            "ack request makes it confirmed",
			ack:             true,
			wantErr:         false,
			wantNilResponse: true,
		},
		{
			name:    "nack request makes it confirmed",
			ack:     false,
			wantErr: true,
		},
		{
			name:            "no automatic response",
			ack:             true,
			wantNilResponse: false,
		},
		{
			name:       "returned request gives error",
			isReturned: true,
			ack:        true,
			wantErr:    true,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := NewRequest().
				WithResponse(!tt.wantNilResponse)

			request.confirmed = make(chan struct{})
			request.response = make(chan *amqp.Delivery)
			request.errChan = make(chan error)
			request.deliveryTag = uint64(i)

			if !tt.isUnknown {
				client.requestsMap.Set(request)
			}

			if tt.isReturned {
				request.returned = &amqp.Return{}
			}

			go func() {
				confirms <- amqp.Confirmation{
					DeliveryTag: request.deliveryTag,
					Ack:         tt.ack,
				}
			}()

			select {
			case <-request.confirmed:
				assert.False(t, tt.wantNoConfirm, "request confirmed")
			case <-time.After(10 * time.Millisecond):
				assert.True(t, tt.wantNoConfirm, "request not confirmed")
			}

			client.requestsMap.Delete(request)

			select {
			case <-request.response:
				assert.True(t, tt.wantNilResponse, "got response")
			case <-time.After(10 * time.Millisecond):
				assert.False(t, tt.wantNilResponse, "no response")
			}

			select {
			case <-request.errChan:
				assert.True(t, tt.wantErr, "got error")
			case <-time.After(10 * time.Millisecond):
				assert.False(t, tt.wantErr, "no error")
			}
		})
	}
}

func TestClientStopWhenCannotStart(t *testing.T) {
	client := NewClient(testURL)

	request := NewRequest().
		WithTimeout(10 * time.Millisecond).
		WithResponse(false)

	_, err := client.Send(request)
	assert.Error(t, err)

	var stopped sync.WaitGroup

	stopped.Add(1)

	go func() {
		client.Stop()
		stopped.Done()
	}()

	assert.Eventually(t, func() bool {
		stopped.Wait()
		return true
	},
		1*time.Second,
		500*time.Millisecond,
	)
}

func TestClientStopWhenNeverStarted(t *testing.T) {
	client := NewClient(testURL)

	var stopped sync.WaitGroup

	stopped.Add(1)

	go func() {
		client.Stop()
		stopped.Done()
	}()

	assert.Eventually(t, func() bool {
		stopped.Wait()
		return true
	},
		1*time.Second,
		500*time.Millisecond,
	)
}

func TestClientConfig(t *testing.T) {
	cert := Certificates{}

	certClient := NewClient(testURL).WithTLS(cert.TLSConfig())
	defer certClient.Stop()

	assert.NotNil(t, certClient, "client with certificate exist")

	ac := amqp.Config{}
	qdSettings := QueueDeclareSettings{}
	cSettings := ConsumeSettings{}
	publSettings := PublishSettings{}

	acClient := NewClient(testURL).WithDialConfig(ac).
		WithQueueDeclareSettings(qdSettings).
		WithPublishSettings(publSettings).
		WithConsumeSettings(cSettings).
		WithConfirmMode(true).
		WithTimeout(2500 * time.Millisecond)
	defer acClient.Stop()

	assert.NotNil(t, acClient, "configured client exist")

	assert.True(t, acClient.publishSettings.ConfirmMode)
	assert.False(t, acClient.publishSettings.Immediate)
	assert.False(t, acClient.publishSettings.Mandatory)
}

func TestClientReconnect(t *testing.T) {
	_, client, start, stop := initTest()
	defer stop()

	start()

	closeConnections(testClientConnectionName)

	// Default wait for the reconnect is 0.5s so 1s should be enough to ensure
	// that the reconnection has gone through.
	time.Sleep(1 * time.Second)

	_, err := client.Send(
		NewRequest().WithRoutingKey(defaultTestQueue),
	)

	assert.NoError(t, err)
}

func TestClientRetry(t *testing.T) {
	tests := []struct {
		name        string
		confirmMode bool
	}{
		{
			name:        "confirm-mode",
			confirmMode: true,
		},
		{
			name:        "no confirm-mode",
			confirmMode: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var conn net.Conn

			dialFunc := func(network, addr string) (net.Conn, error) {
				var err error
				conn, err = amqp.DefaultDial(1*time.Second)(network, addr)

				return conn, err
			}

			_, client, start, stop := initTest()
			defer stop()

			client.
				WithConfirmMode(tt.confirmMode).
				WithDialConfig(amqp.Config{Dial: dialFunc}).
				WithMaxRetries(2)

			start()

			// Closing only for writing ensures that the amqp.Connection doesn't know
			// that it's been closed.
			c, ok := conn.(*net.TCPConn)

			require.True(t, ok)
			require.NoError(t, c.CloseWrite())

			_, err := client.Send(
				NewRequest().
					WithRoutingKey(defaultTestQueue).
					WithResponse(false),
			)

			require.NoError(t, err)

			req := NewRequest().
				WithRoutingKey(defaultTestQueue).
				WithResponse(false)

			// Simulate that we've already retried this one.
			req.numRetries = 2

			c, ok = conn.(*net.TCPConn)

			require.True(t, ok)
			require.NoError(t, c.CloseWrite())

			_, err = client.Send(req)
			require.Error(t, err)
			assert.False(t, errors.Is(err, ErrRequestTimeout))
		})
	}
}

func TestClientTimeout(t *testing.T) {
	server, client, start, stop := initTest()
	defer stop()

	server.Bind(DirectBinding("timeout-queue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		expiration, _ := strconv.Atoi(d.Expiration)

		timeout, ok := d.Headers["timeout"].(bool)
		require.True(t, ok)

		if timeout {
			assert.NotEqual(t, 0, expiration)
		} else {
			assert.Equal(t, 0, expiration)
		}

		time.Sleep(time.Duration(expiration) * time.Millisecond)
	}))

	start()

	cases := []struct {
		name          string
		clientTimeout time.Duration
		request       *Request
		wantTimeout   bool
	}{
		{
			name:          "Client with timeout but no timeout on the Request",
			clientTimeout: 10 * time.Millisecond,
			request:       NewRequest(),
			wantTimeout:   true,
		},
		{
			name:          "Request with timeout but no timeout on the Client",
			clientTimeout: 0,
			request:       NewRequest().WithTimeout(10 * time.Millisecond),
			wantTimeout:   true,
		},
		{
			name:          "Request timeout overrides the Client timeout",
			clientTimeout: 10 * time.Second,
			request:       NewRequest().WithTimeout(10 * time.Millisecond),
			wantTimeout:   true,
		},
		{
			name:          "Request without reply has no timeout",
			clientTimeout: 10 * time.Second,
			request:       NewRequest().WithResponse(false),
			wantTimeout:   false,
		},
	}

	for _, tc := range cases {
		tc.request.WriteHeader("timeout", tc.wantTimeout)
		tc.request.WithRoutingKey("timeout-queue")

		t.Run(tc.name, func(t *testing.T) {
			client.WithTimeout(tc.clientTimeout)

			response, err := client.Send(tc.request)
			if !tc.wantTimeout {
				assert.Nil(t, err)
				return
			}

			assert.Error(t, err)
			assert.True(t, errors.Is(err, ErrRequestTimeout))
			assert.Nil(t, response)
		})
	}
}

func TestClientTimeoutWhileConnecting(t *testing.T) {
	cases := []struct {
		name        string
		client      *Client
		request     *Request
		wantTimeout bool
	}{
		{
			name:        "Client not being able to connect causes real timeout error",
			client:      NewClient("amqp://guest:guest@example:1234/").WithTimeout(10 * time.Millisecond),
			request:     NewRequest().WithResponse(false),
			wantTimeout: true,
		},
		{
			name:        "Client with response not being able to connect causes real timeout error",
			client:      NewClient("amqp://guest:guest@example:1234/").WithTimeout(10 * time.Millisecond),
			request:     NewRequest().WithResponse(true),
			wantTimeout: true,
		},
	}

	for _, tc := range cases {
		tc.request.WriteHeader("timeout", tc.wantTimeout)
		tc.request.WithRoutingKey("myqueue")

		t.Run(tc.name, func(t *testing.T) {
			response, err := tc.client.Send(tc.request)
			if !tc.wantTimeout {
				assert.Nil(t, err)
				return
			}

			assert.Error(t, err)
			assert.True(t, errors.Is(err, ErrRequestTimeout))
			assert.Nil(t, response)
		})
	}
}

func TestGracefulShutdown(t *testing.T) {
	s := NewServer(testURL)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "hello")
	}))

	stop := startAndWait(s)
	defer stop()

	c := NewClient(testURL)
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
	s := NewServer(testURL)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := startAndWait(s)
	defer stop()

	errs := make(chan string, 4)

	c := NewClient(testURL)
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
