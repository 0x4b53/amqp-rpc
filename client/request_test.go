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
	server := server.New()
	server.AddHandler("myqueue", func(ctx context.Context, d *amqp.Delivery) []byte {
		return []byte(fmt.Sprintf("Got message: %s", d.Body))
	})

	go server.ListenAndServe("amqp://guest:guest@localhost:5672/")

	client := New("amqp://guest:guest@localhost:5672/")
	NotEqual(t, client, nil)

	// Test simple form.
	request := NewRequest("myqueue").
		WithResponse(true).
		WithStringBody("hello request")

	response, err := client.Send(request)
	Equal(t, err, nil)
	Equal(t, response.Body, []byte("Got message: hello request"))

	// Test with context, content type nad raw body.
	request = NewRequest("myqueue").
		WithContext(context.TODO()).
		WithResponse(false).
		WithContentType("application/json").
		WithBody([]byte(`{"foo":"bar"}`))

	response, err = client.Send(request)
	Equal(t, err, nil)
	Equal(t, response, nil)
}
