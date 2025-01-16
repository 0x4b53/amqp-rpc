package amqprpc

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequest(t *testing.T) {
	url := "amqp://guest:guest@localhost:5672/"

	s := NewServer(url)
	s.Bind(DirectBinding("myqueue", func(_ context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := startServerAndWait(s)
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
	require.NoError(t, err, "no errors sending request")
	assert.Equal(t, []byte("Got message: hello request"), response.Body, "correct body returned")

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
	require.NoError(t, err, "no errors sending request")
	assert.Nil(t, response, "no body returned when not waiting for replies")

	request = NewRequest().
		WithRoutingKey("myqueue").
		WithBody("original message").
		AddMiddleware(myMiddle)

	response, err = client.Send(request)
	require.NoError(t, err, "no errors sending request")
	assert.NotNil(t, response.Body, "body exist")
	assert.Equal(t, []byte("Got message: middleware message"), response.Body, "correct body returned")
}

func TestRequestWriting(t *testing.T) {
	r := NewRequest().WithRoutingKey("foo")

	assert.Empty(t, r.Publishing.Body, "no body at start")
	assert.Empty(t, r.Publishing.Headers, "no headers at start")

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

func TestRequestTimeout(t *testing.T) {
	tests := []struct {
		name            string
		defaultTimeout  time.Duration
		requestTimeout  time.Duration
		contextTimeout  time.Duration
		wantCtxDeadline time.Duration
	}{
		{
			name: "no timeout when no timeout is set",
		},
		{
			name:            "request timeout is used when set, even when later",
			defaultTimeout:  10 * time.Second,
			requestTimeout:  20 * time.Second,
			contextTimeout:  30 * time.Second,
			wantCtxDeadline: 20 * time.Second,
		},
		{
			name:            "use default when request timeout is not set",
			defaultTimeout:  10 * time.Second,
			requestTimeout:  0,
			contextTimeout:  30 * time.Second,
			wantCtxDeadline: 10 * time.Second,
		},
		{
			name:            "context deadline is used when earlier than request timeout",
			defaultTimeout:  10 * time.Second,
			requestTimeout:  20 * time.Second,
			contextTimeout:  15 * time.Second,
			wantCtxDeadline: 15 * time.Second,
		},
		{
			name:            "context deadline is used when earlier than default timeout",
			defaultTimeout:  10 * time.Second,
			requestTimeout:  0,
			contextTimeout:  5 * time.Second,
			wantCtxDeadline: 5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			wantDeadlineMin := time.Now().Add(tt.wantCtxDeadline)

			if tt.contextTimeout > 0 {
				var ctxCancel context.CancelFunc

				ctx, ctxCancel = context.WithTimeout(context.Background(), tt.contextTimeout)
				defer ctxCancel()
			}

			r := NewRequest().
				WithContext(ctx).
				WithTimeout(tt.requestTimeout)

			cancel := r.startTimeout(tt.defaultTimeout)
			defer cancel()

			wantDeadlineMax := wantDeadlineMin.Add(tt.wantCtxDeadline)

			if tt.wantCtxDeadline == 0 {
				_, ok := r.Context.Deadline()
				require.False(t, ok)
				require.Zero(t, r.Publishing.Expiration)

				return
			}

			{
				// Ensure that the deadline is set correctly.
				deadline, ok := r.Context.Deadline()
				require.True(t, ok)

				require.GreaterOrEqual(t, deadline, wantDeadlineMin)
				require.LessOrEqual(t, deadline, wantDeadlineMax)

				require.NotZero(t, r.Publishing.Expiration)
			}

			{
				// ensure that the expiration is set correctly on the publishing.
				i, err := strconv.ParseInt(r.Publishing.Expiration, 10, 64)
				require.NoError(t, err)

				expireAt := time.Now().Add(time.Duration(i) * time.Millisecond)

				require.GreaterOrEqual(t, expireAt, wantDeadlineMin)
				require.LessOrEqual(t, expireAt, wantDeadlineMax)
			}
		})
	}
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
	c.Sender = func(_ *Request) (*amqp.Delivery, error) {
		// Usually i would send something...
		return &amqp.Delivery{}, nil
	}

	_, err := c.Send(r)

	require.NoError(t, err)
	assert.True(t, changeThroughMiddleware, "requesst changed through middleware")
}

func myMiddle(next SendFunc) SendFunc {
	return func(r *Request) (*amqp.Delivery, error) {
		r.Publishing.Body = []byte("middleware message")

		return next(r)
	}
}
