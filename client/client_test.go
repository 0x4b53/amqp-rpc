package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bombsimon/amqp-rpc/connection"
	"github.com/bombsimon/amqp-rpc/server"
	"github.com/bombsimon/amqp-rpc/test_helpers"
	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestClient(t *testing.T) {
	server := server.New(url)
	server.AddHandler("myqueue", func(ctx context.Context, d amqp.Delivery) []byte {
		return []byte(fmt.Sprintf("Got message: %s", d.Body))
	})

	go server.ListenAndServe()

	client := New("amqp://guest:guest@localhost:5672/")
	NotEqual(t, client, nil)

	request := NewRequest("myqueue").WithStringBody("client testing")
	response, err := client.Send(request)
	Equal(t, err, nil)
	Equal(t, response.Body, []byte("Got message: client testing"))
}

func TestClientConfig(t *testing.T) {
	cert := connection.Certificates{}
	certClient := New(url, cert)

	NotEqual(t, certClient, nil)

	ac := amqp.Config{}
	acClient := New(url, ac)

	NotEqual(t, acClient, nil)
}

func TestReconnect(t *testing.T) {
	dialer, connections := test_helpers.TestDialer(t)
	client := New(url, amqp.Config{Dial: dialer})
	NotEqual(t, client, nil)

	// Hook into the connection, disconnect
	time.Sleep(100 * time.Millisecond)
	conn, _ := <-connections
	conn.Close()

	_, err := client.Send(NewRequest("myqueue").WithStringBody("client testing"))
	NotEqual(t, err, nil)

	// Ensure we're reconnected
	time.Sleep(100 * time.Millisecond)

	_, err = client.Send(NewRequest("myqueue").WithStringBody("client testing").WithResponse(false))
	Equal(t, err != nil, false)
}
