package amqprpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

const (
	testURL                  = "amqp://guest:guest@localhost:5672/"
	serverAPITestURL         = "http://guest:guest@localhost:15672/api"
	defaultTestQueue         = "test.queue"
	testClientConnectionName = "test-client"
	testServerConnectionName = "test-server"
)

// MockAcknowledger is a mocked amqp.Acknowledger, useful for tests.
type MockAcknowledger struct {
	Acks    int
	Nacks   int
	Rejects int
	Closes  int
	OnAckFn func() error
}

// Ack increases Acks.
func (ma *MockAcknowledger) Ack(_ uint64, _ bool) error {
	ma.Acks++

	if ma.OnAckFn != nil {
		return ma.OnAckFn()
	}

	return nil
}

// Nack increases Nacks.
func (ma *MockAcknowledger) Nack(_ uint64, _, _ bool) error {
	ma.Nacks++
	return nil
}

// Reject increases Rejects.
func (ma *MockAcknowledger) Reject(_ uint64, _ bool) error {
	ma.Rejects++
	return nil
}

// Close increses Closes.
func (ma *MockAcknowledger) Close() error {
	ma.Closes++
	return nil
}

// startServerAndWait will start s by running ListenAndServe, it will then block
// until the server is started.
func startServerAndWait(s *Server) func() {
	started := make(chan struct{})
	once := sync.Once{}

	s.OnConnected(func(_, _ *amqp.Connection, _, _ *amqp.Channel) {
		once.Do(func() {
			close(started)
		})
	})

	done := make(chan struct{})

	go func() {
		s.ListenAndServe()
		close(done)
	}()

	<-started

	return func() {
		s.Stop()
		<-done
	}
}

func startClientAndWait(c *Client) {
	started := make(chan struct{})
	once := sync.Once{}

	c.OnConnected(func(_, _ *amqp.Connection, _, _ *amqp.Channel) {
		once.Do(func() {
			close(started)
		})
	})

	c.Connect()

	<-started
}

func deleteQueue(name string) {
	queueURL := fmt.Sprintf("%s/queues/%s/%s", serverAPITestURL, url.PathEscape("/"), url.PathEscape(name))

	req, err := http.NewRequest(http.MethodDelete, queueURL, http.NoBody)
	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}

	_ = resp.Body.Close()
}

func deleteAllQueues(t *testing.T) {
	allQueuesURL := fmt.Sprintf("%s/queues/%s?disable_stats=true", serverAPITestURL, url.PathEscape("/"))

	req, err := http.NewRequest(http.MethodGet, allQueuesURL, http.NoBody)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	var queues []struct {
		Name string `json:"name"`
	}

	err = json.NewDecoder(resp.Body).Decode(&queues)
	require.NoError(t, err)

	for _, queue := range queues {
		deleteQueue(queue.Name)
	}
}

func closeConnections(names ...string) {
	var (
		connectionsURL = fmt.Sprintf("%s/connections", serverAPITestURL)
		connections    []map[string]interface{}
	)

	// It takes a while (0.5s - 4s) for the management plugin to discover the
	// connections so we loop until we've found some.
	for i := 0; i < 20; i++ {
		resp, err := http.Get(connectionsURL)
		if err != nil {
			panic(err)
		}

		err = json.NewDecoder(resp.Body).Decode(&connections)
		if err != nil {
			panic(err)
		}

		_ = resp.Body.Close()

		if len(connections) == 0 {
			time.Sleep(time.Duration(i*100) * time.Millisecond)
			continue
		}

		break
	}

	for _, conn := range connections {
		// Should we close this connection?
		shouldRemove := false

		for _, name := range names {
			if conn["user_provided_name"] == name {
				shouldRemove = true
				break
			}
		}

		if !shouldRemove {
			continue
		}

		connName, ok := conn["name"].(string)
		if !ok {
			panic("name is not a string")
		}

		connectionURL := fmt.Sprintf("%s/connections/%s", serverAPITestURL, url.PathEscape(connName))

		req, err := http.NewRequest(http.MethodDelete, connectionURL, http.NoBody)
		if err != nil {
			panic(err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}

		_ = resp.Body.Close()

		fmt.Println("closed", conn["user_provided_name"])
	}
}

func testServer() *Server {
	server := NewServer(testURL).
		WithDialConfig(
			amqp.Config{
				Properties: amqp.Table{
					"connection_name": testServerConnectionName,
				},
			},
		)

	server.Bind(DirectBinding(defaultTestQueue, func(_ context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	return server
}

func testClient() *Client {
	return NewClient(testURL).
		WithDialConfig(
			amqp.Config{
				Properties: amqp.Table{
					"connection_name": testClientConnectionName,
				},
			},
		)
}

func initTest(t *testing.T) (server *Server, client *Client, start, stop func()) {
	deleteAllQueues(t)

	server = testServer()
	client = testClient()

	var stopServer func()

	stop = func() {
		client.Stop()
		stopServer()
	}

	start = func() {
		stopServer = startServerAndWait(server)

		startClientAndWait(client)
	}

	return
}

// heartbeatFailer is a net.Conn that can be configured to fail heartbeats.
type heartbeatFailer struct {
	net.Conn
	failHeartbeats bool
}

func (c *heartbeatFailer) Write(b []byte) (n int, err error) {
	if c.failHeartbeats {
		// The first byte is the type of frame. The heartbeat frame is 8.
		// Some references:
		// amqp091-go/spec091.go/frameHeartbeat
		// https://github.com/rabbitmq/amqp091-go/blob/main/spec/amqp0-9-1.stripped.extended.xml#L48
		// amqp091-go/write.go/writeFrame()
		// https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf (page 23 chapter 2.3.5 Frame Details)
		if len(b) > 0 && b[0] == 8 {
			return 0, errors.New("heartbeat must fail")
		}
	}

	return c.Conn.Write(b)
}
