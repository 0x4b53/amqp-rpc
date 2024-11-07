package amqprpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopic(t *testing.T) {
	s, c, start, stop := initTest(t)
	defer stop()

	wasCalled := make(chan struct{})

	s.Bind(TopicBinding("my_topic", "foo.#", func(_ context.Context, _ *ResponseWriter, _ amqp.Delivery) {
		wasCalled <- struct{}{}
	}))

	start()

	_, err := c.Send(NewRequest().
		WithRoutingKey("foo.bar.baz").
		WithExchange("amq.topic").
		WithResponse(false))

	require.NoError(t, err)

	select {
	case <-wasCalled:
	// Yay!
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for request")
	}
}

func TestHeaders(t *testing.T) {
	s, c, start, stop := initTest(t)
	defer stop()

	handler := func(_ context.Context, rw *ResponseWriter, _ amqp.Delivery) {
		fmt.Fprintf(rw, "Hello, world")
	}

	h := amqp.Table{
		"x-match": "all",
		"foo":     "bar",
	}

	s.Bind(HeadersBinding("my_queue", h, handler))

	start()

	// Ensure 'somewhere.*' matches 'somewhere.there'.
	response, err := c.Send(NewRequest().WithExchange("amq.match").WithHeaders(amqp.Table{"foo": "bar"}))

	require.NoError(t, err, "no errors occurred")
	assert.Equal(t, []byte("Hello, world"), response.Body, "correct request body")
}

func TestSkipQueueDeclare(t *testing.T) {
	s, c, start, stop := initTest(t)
	defer stop()

	handler := func(_ context.Context, rw *ResponseWriter, _ amqp.Delivery) {
		fmt.Fprintf(rw, "Hello, world")
	}

	queueName := "test-skip-queue-declare"

	s.Bind(DirectBinding(queueName, handler))

	start()

	response, err := c.Send(NewRequest().WithRoutingKey(queueName))
	require.NoError(t, err, "no errors occurred")
	require.Equal(t, []byte("Hello, world"), response.Body, "correct request body")

	stop()

	s = NewServer(testURL)
	s.Bind(
		DirectBinding(queueName, handler).
			WithSkipQueueDeclare(true).
			// Set something different as some queue declare arguments, normally
			// this fails but since we set SkipQueueDeclare it will skip declaring the
			// queue so it won't care.
			WithQueueDeclareArg("x-expires", 33),
	)

	stop = startAndWait(s)
	defer stop()

	response, err = c.Send(NewRequest().WithRoutingKey(queueName))
	require.NoError(t, err, "no errors occurred")
	require.Equal(t, []byte("Hello, world"), response.Body, "correct request body")
}
