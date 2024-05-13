package amqprpc

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendWithReply(t *testing.T) {
	_, client, start, stop := initTest()
	defer stop()

	start()

	reply, err := client.Send(
		NewRequest().
			WithRoutingKey(defaultTestQueue).
			WithBody("this is a message"),
	)

	require.NoError(t, err, "client exist")
	assert.Equal(t, []byte("Got message: this is a message"), reply.Body, "got reply")
}

func TestNoAutomaticAck(t *testing.T) {
	deleteQueue("no-auto-ack") // Ensure queue is clean from the start.

	server, client, start, stop := initTest()
	defer stop()

	server.WithAutoAck(false)

	calls := make(chan struct{}, 2)

	server.Bind(
		DirectBinding("no-auto-ack", func(_ context.Context, _ *ResponseWriter, _ amqp.Delivery) {
			calls <- struct{}{}
		}),
	)

	start()

	_, err := client.Send(
		NewRequest().
			WithRoutingKey("no-auto-ack").
			WithResponse(false),
	)

	require.NoError(t, err)

	// Wait for the first message to arrive.
	select {
	case <-calls:
		// We got the message, now we stop the server without having acked the
		// delivery.
		stop()
	case <-time.After(10 * time.Second):
		t.Fatal("wait time exeeded")
	}

	// Restart the server. This should make RabbitMQ deliver the delivery
	// again.
	start()

	select {
	case <-calls:
		// Nice!
	case <-time.After(10 * time.Second):
		t.Fatal("wait time exeeded")
	}
}

func TestMiddleware(t *testing.T) {
	mw := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
			if queueName, _ := QueueNameFromContext(ctx); queueName == "denied" {
				fmt.Fprint(rw, "routing key 'denied' is not allowed")
				return
			}

			next(ctx, rw, d)
		}
	}

	server, client, start, stop := initTest()
	defer stop()

	server.AddMiddleware(mw)

	server.Bind(DirectBinding("allowed", func(_ context.Context, rw *ResponseWriter, _ amqp.Delivery) {
		fmt.Fprint(rw, "this is allowed")
	}))

	server.Bind(DirectBinding("denied", func(_ context.Context, rw *ResponseWriter, _ amqp.Delivery) {
		fmt.Fprint(rw, "this is not allowed")
	}))

	start()

	reply, err := client.Send(
		NewRequest().WithRoutingKey("allowed"),
	)

	require.NoError(t, err, "no error")
	assert.Equal(t, []byte("this is allowed"), reply.Body, "allowed middleware callable")

	reply, err = client.Send(
		NewRequest().WithRoutingKey("denied"),
	)

	require.NoError(t, err, "no error")
	assert.Equal(t, []byte("routing key 'denied' is not allowed"), reply.Body, "denied middleware not callable")
}

func TestServerReconnect(t *testing.T) {
	s := NewServer(testURL).
		WithDialConfig(amqp.Config{
			Properties: amqp.Table{
				"connection_name": "server-reconnect-test",
			},
		}).
		WithAutoAck(false)

	s.Bind(DirectBinding("myqueue", func(_ context.Context, rw *ResponseWriter, d amqp.Delivery) {
		_ = d.Ack(false)

		fmt.Fprintf(rw, "Hello")
	}))

	stop := startAndWait(s)
	defer stop()

	c := NewClient(testURL)
	defer c.Stop()

	request := NewRequest().WithRoutingKey("myqueue")
	reply, err := c.Send(request)
	require.NoError(t, err)
	assert.Equal(t, []byte("Hello"), reply.Body)

	closeConnections("server-reconnect-test")

	request = NewRequest().WithRoutingKey("myqueue")
	reply, err = c.Send(request)
	require.NoError(t, err)
	assert.Equal(t, []byte("Hello"), reply.Body)
}

func TestManualRestart(t *testing.T) {
	hasStarted := make(chan struct{})
	restartChan := make(chan struct{})

	s := NewServer(testURL).
		WithRestartChan(restartChan).
		WithDebugLogger(log.Printf).
		WithAutoAck(false)

	s.OnStarted(func(_, _ *amqp.Connection, _, _ *amqp.Channel) {
		hasStarted <- struct{}{}
	})

	s.Bind(DirectBinding("myqueue", func(_ context.Context, rw *ResponseWriter, d amqp.Delivery) {
		_ = d.Ack(false)

		fmt.Fprintf(rw, "Hello")
	}))

	// Wait for the initial startup signal.
	go func() { <-hasStarted }()

	stop := startAndWait(s)
	defer stop()

	c := NewClient(testURL)
	defer c.Stop()

	request := NewRequest().WithRoutingKey("myqueue")
	reply, err := c.Send(request)
	require.NoError(t, err)
	assert.Equal(t, []byte("Hello"), reply.Body)

	s.Restart()
	<-hasStarted

	request = NewRequest().WithRoutingKey("myqueue")
	reply, err = c.Send(request)
	require.NoError(t, err)
	assert.Equal(t, []byte("Hello"), reply.Body)
}

func TestServerOnStarted(t *testing.T) {
	errs := make(chan string, 4)

	s := NewServer(testURL)
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

func TestServerConfig(t *testing.T) {
	s := NewServer(testURL)
	assert.NotNil(t, s)
	assert.True(t, s.exchangeDeclareSettings.Durable)
	assert.Equal(t, 10, s.consumeSettings.QoSPrefetchCount)

	qdSettings := QueueDeclareSettings{
		DeleteWhenUnused: true,
		Durable:          true,
	}
	cSettings := ConsumeSettings{
		QoSPrefetchCount: 20,
		QoSPrefetchSize:  100,
		Consumer:         "myconsumer",
	}
	eSettings := ExchangeDeclareSettings{
		Durable:    false,
		AutoDelete: true,
	}

	s.WithQueueDeclareSettings(qdSettings).
		WithConsumeSettings(cSettings).
		WithExchangeDeclareSettings(eSettings)

	assert.Equal(t, s.queueDeclareSettings, qdSettings)
	assert.Equal(t, s.consumeSettings, cSettings)
	assert.Equal(t, s.exchangeDeclareSettings, eSettings)
}

func TestContextDoneWhenServerStopped(t *testing.T) {
	server, client, start, stop := initTest()

	isShuttingDown := make(chan bool, 1)

	server.Bind(DirectBinding("context.test", func(ctx context.Context, _ *ResponseWriter, _ amqp.Delivery) {
		shutdownCh, ok := ShutdownChanFromContext(ctx)
		require.True(t, ok)

		select {
		case <-shutdownCh:
			isShuttingDown <- true
		case <-time.After(5 * time.Second):
			isShuttingDown <- false
		}
	}))

	start()

	_, err := client.Send(
		NewRequest().
			WithRoutingKey("context.test").
			WithResponse(false),
	)

	require.NoError(t, err)

	stop()

	select {
	case wasDone := <-isShuttingDown:
		assert.True(t, wasDone)
	case <-time.After(10 * time.Second):
		t.Fatalf("handler was never called")
	}
}
