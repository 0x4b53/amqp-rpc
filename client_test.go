package amqprpc

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	_, client, start, stop := initTest(t)
	defer stop()

	start()

	request := NewRequest().WithRoutingKey(defaultTestQueue).WithBody("client testing")
	response, err := client.Send(request)

	require.NoError(t, err)
	assert.Equal(t, []byte("Got message: client testing"), response.Body, "correct body in response")
}

func TestClientNoConfirmMode(t *testing.T) {
	_, client, start, stop := initTest(t)
	defer stop()

	client.WithConfirmMode(false)

	start()

	request := NewRequest().WithRoutingKey(defaultTestQueue).WithBody("client testing")
	response, err := client.Send(request)

	require.NoError(t, err)
	assert.Equal(t, []byte("Got message: client testing"), response.Body, "correct body in response")
}

func TestClientDataRace(t *testing.T) {
	_, client, start, stop := initTest(t)
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
	_, client, start, stop := initTest(t)
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
			require.ErrorIs(t, err, ErrRequestReturned)
			assert.Contains(t, err.Error(), "NO_ROUTE")
		})
	}
}

func TestClient_ConfirmsConsumer(t *testing.T) {
	t.Parallel()

	client := NewClient("")
	client.requestsMap = RequestMap{
		byDeliveryTag:   make(map[uint64]*Request),
		byCorrelationID: make(map[string]*Request),
	}

	var nextDeliveryTag uint64 = 0

	returns := make(chan amqp.Return)
	confirms := make(chan amqp.Confirmation)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go client.runConfirmsConsumer(confirms, returns, &wg)

	t.Cleanup(func() {
		close(confirms)
		close(returns)
	})

	makeRequest := func() *Request {
		deliveryTag := atomic.AddUint64(&nextDeliveryTag, 1)

		request := NewRequest()
		request.response = make(chan response)
		request.deliveryTag = deliveryTag
		request.Publishing.CorrelationId = uuid.NewString()
		client.requestsMap.Set(request)

		return request
	}

	t.Run("returns", func(t *testing.T) {
		t.Run("unknown CorrelationId does nothing", func(t *testing.T) {
			r := makeRequest()

			returns <- amqp.Return{
				CorrelationId: uuid.NewString(),
			}

			select {
			case <-time.After(30 * time.Millisecond):
			case <-r.response:
				t.Fatal("got response")
			}
		})

		t.Run("sets request.returned", func(t *testing.T) {
			r := makeRequest()

			returns <- amqp.Return{
				CorrelationId: r.Publishing.CorrelationId,
				ReplyCode:     404,
				ReplyText:     "NO_ROUTE",
			}

			confirms <- amqp.Confirmation{
				DeliveryTag: r.deliveryTag,
				Ack:         true,
			}

			select {
			case res := <-r.response:
				require.ErrorIs(t, res.err, ErrRequestReturned)
				require.Nil(t, res.delivery)
				require.ErrorContains(t, res.err, "404")
				require.ErrorContains(t, res.err, "NO_ROUTE")
			case <-time.After(5 * time.Second):
				t.Fatal("got no response")
			}
		})
	})

	t.Run("confirms", func(t *testing.T) {
		t.Run("unknown DeliveryTag does nothing", func(t *testing.T) {
			r := makeRequest()

			confirms <- amqp.Confirmation{
				DeliveryTag: 1338 * 1200,
			}

			select {
			case <-time.After(30 * time.Millisecond):
			case <-r.response:
				t.Fatal("got response")
			}
		})

		t.Run("responds directly when the request wants no reply", func(t *testing.T) {
			r := makeRequest().WithResponse(false)

			confirms <- amqp.Confirmation{
				DeliveryTag: r.deliveryTag,
				Ack:         true,
			}

			select {
			case res := <-r.response:
				require.NoError(t, res.err)
				require.Nil(t, res.delivery)
			case <-time.After(5 * time.Second):
				t.Fatal("got no response")
			}
		})

		t.Run("no response when the request wants reply", func(t *testing.T) {
			r := makeRequest().WithResponse(true)

			confirms <- amqp.Confirmation{
				DeliveryTag: r.deliveryTag,
				Ack:         true,
			}

			select {
			case <-time.After(30 * time.Millisecond):
			case <-r.response:
				t.Fatal("got response")
			}
		})

		t.Run("nack will respond with ErrRequestRejected", func(t *testing.T) {
			r := makeRequest()

			confirms <- amqp.Confirmation{
				DeliveryTag: r.deliveryTag,
				Ack:         false,
			}

			select {
			case res := <-r.response:
				require.ErrorIs(t, res.err, ErrRequestRejected)
				require.Nil(t, res.delivery)
			case <-time.After(5 * time.Second):
				t.Fatal("got no response")
			}
		})
	})

	t.Run("closing returns will not stop select", func(t *testing.T) {
		confirms := make(chan amqp.Confirmation)
		returns := make(chan amqp.Return)

		wg := sync.WaitGroup{}
		wg.Add(1)

		go client.runConfirmsConsumer(confirms, returns, &wg)

		close(returns)

		// Confirms are still consumed until the channel is closed.
		{
			r := makeRequest()
			confirms <- amqp.Confirmation{
				DeliveryTag: r.deliveryTag,
				Ack:         false,
			}

			select {
			case <-r.response:
			case <-time.After(5 * time.Second):
				t.Fatal("got no response")
			}
		}

		close(confirms)

		finished := make(chan struct{})

		go func() {
			wg.Wait()
			close(finished)
		}()

		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatal("did not exit")
		}
	})
}

func TestClientStop(t *testing.T) {
	t.Parallel()

	client := NewClient(testURL)

	request := NewRequest().
		WithResponse(false)

	// Ensure that the client has started.
	_, err := client.Send(request)
	require.ErrorIs(t, err, ErrRequestReturned)

	stopped := make(chan struct{})

	go func() {
		client.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("did not exit")
	}
}

func TestClientStopWhenCannotStart(t *testing.T) {
	t.Parallel()

	client := NewClient(testURL)

	request := NewRequest().
		WithTimeout(10 * time.Millisecond).
		WithResponse(false)

	_, err := client.Send(request)
	require.Error(t, err)

	stopped := make(chan struct{})

	go func() {
		client.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("did not exit")
	}
}

func TestClientStopWhenNeverStarted(t *testing.T) {
	t.Parallel()

	client := NewClient(testURL)

	stopped := make(chan struct{})

	go func() {
		client.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("did not exit")
	}
}

func TestClientConfig(t *testing.T) {
	cert := Certificates{}

	certClient := NewClient(testURL).WithTLS(cert.TLSConfig())
	defer certClient.Stop()

	assert.NotNil(t, certClient, "client with certificate exist")

	ac := amqp.Config{}

	acClient := NewClient(testURL).
		WithDialConfig(ac).
		WithConfirmMode(true).
		WithTimeout(2500 * time.Millisecond)
	defer acClient.Stop()

	assert.NotNil(t, acClient, "configured client exist")
	assert.True(t, acClient.confirmMode)
}

func TestClientReconnect(t *testing.T) {
	_, client, start, stop := initTest(t)
	defer stop()

	start()

	closeConnections(testClientConnectionName)

	// Default wait for the reconnect is 0.5s so 1s should be enough to ensure
	// that the reconnection has gone through.
	time.Sleep(1 * time.Second)

	_, err := client.Send(
		NewRequest().WithRoutingKey(defaultTestQueue),
	)

	require.NoError(t, err)
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

			_, client, start, stop := initTest(t)
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
			assert.NotErrorIs(t, err, ErrRequestTimeout)
		})
	}
}

func TestClientTimeout(t *testing.T) {
	server, client, start, stop := initTest(t)
	defer stop()

	server.Bind(DirectBinding("timeout-queue", func(_ context.Context, _ *ResponseWriter, d amqp.Delivery) {
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
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, ErrRequestTimeout)
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
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, ErrRequestTimeout)
			assert.Nil(t, response)
		})
	}
}

func TestGracefulShutdown(t *testing.T) {
	s := NewServer(testURL)
	s.Bind(DirectBinding("myqueue", func(_ context.Context, rw *ResponseWriter, _ amqp.Delivery) {
		fmt.Fprintf(rw, "hello")
	}))

	stop := startServerAndWait(s)
	defer stop()

	c := NewClient(testURL)
	defer c.Stop()

	r, err := c.Send(NewRequest().WithRoutingKey("myqueue"))

	require.NoError(t, err, "no error before shutting down")
	assert.Equal(t, "hello", string(r.Body), "correct body")

	c.Stop()

	// We should have a new synx.Once and reconnect after a disconnect.
	time.Sleep(50 * time.Millisecond)

	r, err = c.Send(NewRequest().WithRoutingKey("myqueue"))
	require.NoError(t, err, "no error when sending after stop")
	assert.Equal(t, "hello", string(r.Body), "correct body after sending after stop")
}

func TestClient_OnConnected(t *testing.T) {
	s := NewServer(testURL)
	s.Bind(DirectBinding("myqueue", func(_ context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := startServerAndWait(s)
	defer stop()

	errs := make(chan string, 4)

	c := NewClient(testURL)
	defer c.Stop()

	c.OnConnected(func(inC, outC *amqp.Connection, inCh, outCh *amqp.Channel) {
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

	// Since the client is lazy, OnConnected isn't called until the first .Send().
	request := NewRequest().
		WithRoutingKey("myqueue").
		WithBody("client testing").
		WithResponse(false)

	_, err := c.Send(request)
	require.NoError(t, err)

	select {
	case e, ok := <-errs:
		if ok {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Error("OnConnected was never called")
	}
}

func TestClientOnErrorFunc(t *testing.T) {
	t.Run("OnErrorFunc called when auth error", func(t *testing.T) {
		t.Parallel()

		c := NewClient(strings.Replace(testURL, "guest:guest", "guest:wrong", 1))

		t.Cleanup(func() {
			c.Stop()
		})

		var wasCalled atomic.Bool

		c.OnError(func(err error) {
			assert.ErrorIs(t, err, ErrConnectFailed)

			wasCalled.Store(true)
		})

		c.Connect()

		require.Eventually(t, wasCalled.Load, 5*time.Second, time.Millisecond)
	})

	t.Run("OnErrorFunc is called when connection closed unexpectedly", func(t *testing.T) {
		t.Parallel()

		c := NewClient(testURL)

		t.Cleanup(func() {
			c.Stop()
		})

		var wasCalled atomic.Bool

		c.OnError(func(err error) {
			assert.ErrorIs(t, err, ErrUnexpectedConnClosed)

			wasCalled.Store(true)
		})

		var conn atomic.Pointer[amqp.Connection]

		c.OnConnected(func(c, _ *amqp.Connection, _, _ *amqp.Channel) {
			conn.Store(c)
		})

		c.Connect()

		// Wait for the client to be connected.
		require.Eventually(t, func() bool { return conn.Load() != nil }, 5*time.Second, time.Millisecond)

		// Close the connection to trigger the error.
		require.NoError(t, conn.Load().Close())

		require.Eventually(t, wasCalled.Load, 5*time.Second, time.Millisecond)
	})

	t.Run("don't reconnect if client stopped in OnErrorFunc", func(t *testing.T) {
		t.Parallel()

		c := NewClient(strings.Replace(testURL, "guest:guest", "guest:wrong", 1))

		ch := make(chan struct{})

		c.OnError(func(_ error) {
			ch <- struct{}{}

			// Note the goroutine here.
			go c.Stop()
		})

		c.Connect()

		select {
		case <-ch: // expected
		case <-time.After(5 * time.Second):
			t.Fatal("expected error but didn't get one")
		}

		select {
		case <-ch:
			t.Fatal("tried to connect after stop() called")
		case <-time.After(time.Second):
		}
	})
}
