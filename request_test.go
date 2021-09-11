package amqprpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequest(t *testing.T) {
	var (
		assert = assert.New(t)
		url    = "amqp://guest:guest@localhost:5672/"
	)

	s := NewServer(url)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := startAndWait(s)
	defer stop()

	client := NewClient(url)
	defer client.Stop()

	assert.NotNil(t, client, "client exist")

	// Test simple form.
	request := NewRequest().
		WithRoutingKey("myqueue").
		WithResponse(true).
		WithBody("hello request")

	response, err := client.Send(request)
	assert.Nil(err, "no errors sending request")
	assert.Equal([]byte("Got message: hello request"), response.Body, "correct body returned")

	// Test with exchange, headers, content type, correlation ID and raw body.
	request = NewRequest().
		WithRoutingKey("myqueue").
		WithExchange("").
		WithHeaders(amqp.Table{}).
		WithResponse(false).
		WithContentType("application/json").
		WithCorrelationID("this-is-unique").
		WithBody(`{"foo":"bar"}`)

	response, err = client.Send(request)
	assert.Nil(err, "no errors sending request")
	assert.Nil(response, "no body returned when not waiting for replies")

	request = NewRequest().
		WithRoutingKey("myqueue").
		WithBody("original message").
		AddMiddleware(myMiddle)

	response, err = client.Send(request)
	assert.Nil(err, "no errors sending request")
	assert.NotNil(response.Body, "body exist")
	assert.Equal([]byte("Got message: middleware message"), response.Body, "correct body returned")
}

func TestRequestWriting(t *testing.T) {
	r := NewRequest().WithRoutingKey("foo")

	assert.Equal(t, 0, len(r.Publishing.Body), "no body at start")
	assert.Equal(t, 0, len(r.Publishing.Headers), "no headers at start")

	t.Run("body writing", func(tt *testing.T) {
		fmt.Fprintf(r, "my body is foo")
		assert.Equal(tt, []byte("my body is foo"), r.Publishing.Body, "correct body written")

		fmt.Fprintf(r, "\nand bar")
		assert.Equal(tt, []byte("my body is foo\nand bar"), r.Publishing.Body, "correct body written")

		r.WithBody("overwrite")
		assert.Equal(tt, []byte("overwrite"), r.Publishing.Body, "correct body written")

		fmt.Fprintf(r, "written")
		assert.Equal(tt, []byte("overwritewritten"), r.Publishing.Body, "correct body written")
	})

	t.Run("header writing", func(tt *testing.T) {
		r.WriteHeader("foo", "bar")
		assert.Equal(tt, amqp.Table{"foo": "bar"}, r.Publishing.Headers, "correct headers written")

		r.WriteHeader("baz", "baa")
		assert.Equal(tt, amqp.Table{"foo": "bar", "baz": "baa"}, r.Publishing.Headers, "correct headers written")

		r.WithHeaders(amqp.Table{"overwritten": "headers"})

		assert.Equal(tt, amqp.Table{"overwritten": "headers"}, r.Publishing.Headers, "correct headers written")

		r.WriteHeader("baz", "foo")
		assert.Equal(tt, amqp.Table{"overwritten": "headers", "baz": "foo"}, r.Publishing.Headers, "correct headers written")
	})
}

func TestRequestContext(t *testing.T) {
	type ctxtype string

	ctxKey := ctxtype("charger")
	changeThroughMiddleware := false

	myMiddleFunc := func(next SendFunc) SendFunc {
		return func(r *Request) (*amqp.Delivery, error) {
			var ok bool
			if changeThroughMiddleware, ok = r.Context.Value(ctxKey).(bool); !ok {
				require.FailNow(t, "failed to assert context")
			}

			return next(r)
		}
	}

	ctx := context.WithValue(context.Background(), ctxKey, true)
	r := NewRequest().WithContext(ctx)

	c := NewClient("").AddMiddleware(myMiddleFunc)
	c.Sender = func(r *Request) (*amqp.Delivery, error) {
		// Usually i would send something...
		return &amqp.Delivery{}, nil
	}

	_, err := c.Send(r)

	assert.Nil(t, err)
	assert.Equal(t, true, changeThroughMiddleware, "requesst changed through middleware")
}

func myMiddle(next SendFunc) SendFunc {
	return func(r *Request) (*amqp.Delivery, error) {
		r.Publishing.Body = []byte("middleware message")

		return next(r)
	}
}
