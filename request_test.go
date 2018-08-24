package amqprpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/streadway/amqp"
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
	request := NewRequest("myqueue").
		WithResponse(true).
		WithBody("hello request")

	response, err := client.Send(request)
	Equal(t, err, nil)
	Equal(t, response.Body, []byte("Got message: hello request"))

	// Test with exchange, headers, content type nad raw body.
	request = NewRequest("myqueue").
		WithExchange("").
		WithHeaders(amqp.Table{}).
		WithResponse(false).
		WithContentType("application/json").
		WithBody(`{"foo":"bar"}`)

	response, err = client.Send(request)
	Equal(t, err, nil)
	Equal(t, response, nil)

	request = NewRequest("myqueue").
		WithBody("original message").
		AddMiddleware(myMiddle)

	response, err = client.Send(request)
	Equal(t, err, nil)
	NotEqual(t, response.Body, []byte("Got message: original message"))
	Equal(t, response.Body, []byte("Got message: middleware message"))
}

func TestRequestWriting(t *testing.T) {
	r := NewRequest("foo")
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

func myMiddle(next SendFunc) SendFunc {
	return func(r *Request) (*amqp.Delivery, error) {
		r.Publishing.Body = []byte("middleware message")

		return next(r)
	}
}
