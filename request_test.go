package amqprpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	. "gopkg.in/go-playground/assert.v1"
)

func TestRequest(t *testing.T) {
	var url = "amqp://guest:guest@localhost:5672/"

	s := NewServer(url)
	s.Bind(DirectBinding("myqueue", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	stop := startAndWait(s)
	defer stop()

	client := NewClient(url)
	NotEqual(t, client, nil)

	// Test simple form.
	request := NewRequest().
		WithRoutingKey("myqueue").
		WithResponse(true).
		WithBody("hello request")

	response, err := client.Send(request)
	Equal(t, err, nil)
	Equal(t, response.Body, []byte("Got message: hello request"))

	// Test with exchange, headers, content type nad raw body.
	request = NewRequest().
		WithRoutingKey("myqueue").
		WithExchange("").
		WithHeaders(amqp.Table{}).
		WithResponse(false).
		WithContentType("application/json").
		WithBody(`{"foo":"bar"}`)

	response, err = client.Send(request)
	Equal(t, err, nil)
	Equal(t, response, nil)

	request = NewRequest().
		WithRoutingKey("myqueue").
		WithBody("original message").
		AddMiddleware(myMiddle)

	response, err = client.Send(request)
	Equal(t, err, nil)
	NotEqual(t, response.Body, []byte("Got message: original message"))
	Equal(t, response.Body, []byte("Got message: middleware message"))
}

func TestRequestWriting(t *testing.T) {
	r := NewRequest().WithRoutingKey("foo")

	Equal(t, len(r.Publishing.Body), 0)
	Equal(t, len(r.Publishing.Headers), 0)

	t.Run("body writing", func(tt *testing.T) {
		fmt.Fprintf(r, "my body is foo")
		Equal(tt, r.Publishing.Body, []byte("my body is foo"))

		fmt.Fprintf(r, "\nand bar")
		Equal(tt, r.Publishing.Body, []byte("my body is foo\nand bar"))

		r.WithBody("overwrite")
		Equal(tt, r.Publishing.Body, []byte("overwrite"))

		fmt.Fprintf(r, "written")
		Equal(tt, r.Publishing.Body, []byte("overwritewritten"))
	})

	t.Run("header writing", func(tt *testing.T) {
		r.WriteHeader("foo", "bar")
		Equal(tt, r.Publishing.Headers, amqp.Table{
			"foo": "bar",
		})

		r.WriteHeader("baz", "baa")
		Equal(tt, r.Publishing.Headers, amqp.Table{
			"foo": "bar",
			"baz": "baa",
		})

		r.WithHeaders(amqp.Table{
			"overwritten": "headers",
		})
		Equal(tt, r.Publishing.Headers, amqp.Table{
			"overwritten": "headers",
		})

		r.WriteHeader("baz", "foo")
		Equal(tt, r.Publishing.Headers, amqp.Table{
			"overwritten": "headers",
			"baz":         "foo",
		})
	})
}

func TestRequestContext(t *testing.T) {
	changeThroughMiddleware := false

	myMiddle := func(next SendFunc) SendFunc {
		return func(r *Request) (*amqp.Delivery, error) {
			changeThroughMiddleware = r.Context.Value("changer").(bool)

			return next(r)
		}
	}

	ctx := context.WithValue(context.Background(), "changer", true)
	r := NewRequest().WithContext(ctx)

	c := NewClient("").AddMiddleware(myMiddle)
	c.Sender = func(r *Request) (*amqp.Delivery, error) {
		// Usually i would send something...
		return &amqp.Delivery{}, nil
	}

	c.Send(r)

	Equal(t, changeThroughMiddleware, true)
}

func TestStream(t *testing.T) {
	var (
		url        = "amqp://guest:guest@localhost:5672/"
		assert     = assert.New(t)
		numServers = 2
	)

	// Create two servers on a fanout exchange that will both reply.
	for range make([]int, numServers) {
		s := NewServer(url)
		s.Bind(FanoutBinding("myexchange", func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
			fmt.Fprintf(rw, "Responding")
		}))

		defer startAndWait(s)()
	}

	client := NewClient(url)

	request := NewRequest().
		WithExchange("myexchange").
		WithStream()

	loopFunc := func(shouldTimeout bool) {
		for range make([]int, numServers) {
			select {
			case d, _ := <-request.Stream():
				assert.NotEqual(nil, d)
				assert.Equal("Responding", string(d.Body))
			case <-time.After(100 * time.Millisecond):
				if shouldTimeout {
					assert.Equal(nil, nil, "no stream recevied as expected")
				} else {
					assert.Fail("did not get a response")
				}
			}
		}
	}

	response, err := client.Send(request)
	assert.Equal(true, response == nil)
	assert.Equal(nil, err)

	loopFunc(false)

	// Possible to re-use request.
	response, err = client.Send(request)
	assert.Equal(true, response == nil)
	assert.Equal(nil, err)

	loopFunc(false)

	assert.Equal(2, len(request.correlationIDs), "after two send we've got two corr IDs")

	// End session
	request.EndStream()

	assert.Equal(0, len(request.correlationIDs), "after endign the stream we cleared the history")

	response, err = client.Send(request)
	assert.Equal(true, response != nil, "we got a response from Send")
	assert.Equal(nil, err)

	loopFunc(true)
}

func myMiddle(next SendFunc) SendFunc {
	return func(r *Request) (*amqp.Delivery, error) {
		r.Publishing.Body = []byte("middleware message")

		return next(r)
	}
}
