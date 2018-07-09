package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bombsimon/amqp-rpc/connection"
	"github.com/bombsimon/amqp-rpc/server"
	"github.com/bombsimon/amqp-rpc/testhelpers"
	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestClient(t *testing.T) {
	s := server.New(url)
	s.Bind(server.DirectBinding("myqueue", func(ctx context.Context, rw *server.ResponseWriter, d amqp.Delivery) {
		fmt.Fprintf(rw, "Got message: %s", d.Body)
	}))

	go s.ListenAndServe()
	time.Sleep(50 * time.Millisecond)

	client := New("amqp://guest:guest@localhost:5672/")
	NotEqual(t, client, nil)

	request := NewRequest("myqueue").WithStringBody("client testing")
	response, err := client.Send(request)
	Equal(t, err, nil)
	Equal(t, response.Body, []byte("Got message: client testing"))
}

func TestClientConfig(t *testing.T) {
	cert := connection.Certificates{}
	certClient := New(url).WithTLS(cert)

	NotEqual(t, certClient, nil)

	ac := amqp.Config{}
	qdSettings := connection.QueueDeclareSettings{}
	cSettings := connection.ConsumeSettings{}

	acClient := New(url).WithDialConfig(ac).
		WithQueueDeclareSettings(qdSettings).
		WithConsumeSettings(cSettings).
		WithTimeout(2500 * time.Millisecond)

	NotEqual(t, acClient, nil)
}

func TestReconnect(t *testing.T) {
	dialer, connections := testhelpers.TestDialer(t)
	client := New(url).WithDialConfig(amqp.Config{Dial: dialer})
	NotEqual(t, client, nil)

	// Force a connection by calling send.
	_, err := client.Send(NewRequest("myqueue").WithResponse(false))
	Equal(t, err, nil)

	// Hook into the connection, disconnect
	time.Sleep(100 * time.Millisecond)
	conn, _ := <-connections
	conn.Close()

	_, err = client.Send(NewRequest("myqueue").WithStringBody("client testing"))
	NotEqual(t, err, nil)

	// Ensure we're reconnected
	time.Sleep(100 * time.Millisecond)

	_, err = client.Send(NewRequest("myqueue").WithStringBody("client testing").WithResponse(false))
	Equal(t, err != nil, false)
}
