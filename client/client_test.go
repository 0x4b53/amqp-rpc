package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bombsimon/amqp-rpc/connection"
	"github.com/bombsimon/amqp-rpc/server"
	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

func TestClient(t *testing.T) {
	server := server.New()
	server.AddHandler("myqueue", func(ctx context.Context, d *amqp.Delivery) []byte {
		return []byte(fmt.Sprintf("Got message: %s", d.Body))
	})

	go server.ListenAndServe("amqp://guest:guest@localhost:5672/")

	client := New("amqp://guest:guest@localhost:5672/")
	NotEqual(t, client, nil)

	request := NewRequest("myqueue").WithStringBody("client testing")
	response, err := client.Send(request)
	Equal(t, err, nil)
	Equal(t, response.Body, []byte("Got message: client testing"))
}

func TestExistingConnection(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	Equal(t, err, nil)

	client := NewWithConnection(conn)
	request := NewRequest("non-existing-queue").WithStringBody("ignore reply").WithResponse(false)
	response, err := client.Send(request)
	Equal(t, response, nil)
	Equal(t, err, nil)
}

func TestClientConfig(t *testing.T) {
	cert := connection.Certificates{}
	certClient := New("amqp://guest:guest@localhost:5672/", cert)

	NotEqual(t, certClient, nil)

	ac := amqp.Config{}
	acClient := New("amqp://guest:guest@localhost:5672/", ac)

	NotEqual(t, acClient, nil)
}

func TestReconnect(t *testing.T) {
	client := New("amqp://guest:guest@localhost:5672/")
	NotEqual(t, client, nil)

	// Hook into the connection, disconnect
	connection.GetConnection().Close()

	_, err := client.Send(NewRequest("myqueue").WithStringBody("client testing"))
	NotEqual(t, err, nil)

	// Ensure we're reconnected
	time.Sleep(100 * time.Millisecond)

	_, err = client.Send(NewRequest("myqueue").WithStringBody("client testing").WithResponse(false))
	Equal(t, err != nil, false)
}
