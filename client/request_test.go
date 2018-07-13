package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/bombsimon/amqp-rpc/server"
	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

func TestRequest(t *testing.T) {
	var url = "amqp://guest:guest@localhost:5672/"

	s := server.New(url)
	s.Bind(server.DirectBinding("myqueue", func(ctx context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	go s.ListenAndServe()

	client := New(url)
	NotEqual(t, client, nil)

	// Test simple form.
	request := NewRequest("myqueue").
		WithResponse(true).
		WithStringBody("hello request")

	response, err := client.Send(request)
	Equal(t, err, nil)
	Equal(t, response.Body, []byte("Got message: hello request"))

	// Test with exchange, headers, content type nad raw body.
	request = NewRequest("myqueue").
		WithExchange("").
		WithHeaders(amqp.Table{}).
		WithResponse(false).
		WithContentType("application/json").
		WithBody([]byte(`{"foo":"bar"}`))

	response, err = client.Send(request)
	Equal(t, err, nil)
	Equal(t, response, nil)

	request = NewRequest("myqueue").
		WithStringBody("original message").
		AddMiddleware(myMiddle)

	response, err = client.Send(request)
	Equal(t, err, nil)
	NotEqual(t, response.Body, []byte("Got message: original message"))
	Equal(t, response.Body, []byte("Got message: middleware message"))
}

func myMiddle(next SendFunc) SendFunc {
	return func(r *Request) (*amqp.Delivery, error) {
		r.Body = []byte("middleware message")

		return next(r)
	}
}
