package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/bombsimon/amqp-rpc/client"
	"github.com/bombsimon/amqp-rpc/connection"
	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestSendWithReply(t *testing.T) {
	cert := connection.Certificates{}

	s := New(url).WithDialConfig(amqp.Config{
		Dial:            connection.DefaultDialer,
		TLSClientConfig: cert.TLSConfig(),
	})

	NotEqual(t, s.dialconfig.TLSClientConfig, nil)

	s.AddHandler("myqueue", func(ctx context.Context, d *amqp.Delivery) []byte {
		return []byte(fmt.Sprintf("Got message: %s", d.Body))
	})

	go s.ListenAndServe()

	c := client.New(url)
	request := client.NewRequest("myqueue").WithStringBody("this is a message")
	reply, err := c.Send(request)

	Equal(t, err, nil)
	Equal(t, reply.Body, []byte("Got message: this is a message"))
}

func TestMiddleware(t *testing.T) {
	mw := func(rk string, ctx context.Context, d *amqp.Delivery) error {
		if rk == "denied" {
			return errors.New("routing key 'denied' is not allowed")
		}

		return nil
	}

	s := New(url).AddMiddleware(mw)

	s.AddHandler("allowed", func(ctx context.Context, d *amqp.Delivery) []byte {
		return []byte(fmt.Sprintf("this is allowed"))
	})

	s.AddHandler("denied", func(ctx context.Context, d *amqp.Delivery) []byte {
		return []byte(fmt.Sprintf("this is not allowed"))
	})

	go s.ListenAndServe()

	c := client.New(url)

	request := client.NewRequest("allowed")
	reply, err := c.Send(request)

	Equal(t, err, nil)
	Equal(t, reply.Body, []byte("this is allowed"))

	request = client.NewRequest("denied")
	reply, err = c.Send(request)

	Equal(t, err, nil)
	Equal(t, reply.Body, []byte("routing key 'denied' is not allowed"))
}

func TestReconnect(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	Equal(t, err, nil)
	defer conn.Close()

	ch, err := conn.Channel()
	Equal(t, err, nil)
	defer ch.Close()

	ch.QueueDelete(
		"myqueue",
		false, // ifUnused
		false, // ifEmpty
		false, // noWait
	)

	dialer, getNetConn := testDialer(t)
	s := New(url).WithDialConfig(amqp.Config{Dial: dialer})

	s.AddHandler("myqueue", func(ctx context.Context, d *amqp.Delivery) []byte {
		return []byte(fmt.Sprintf("Got message: %s", d.Body))
	})

	go s.ListenAndServe()

	// Sleep a bit to ensure server is started.
	time.Sleep(50 * time.Millisecond)
	c := client.New(url)

	for i := 0; i < 2; i++ {
		message := []byte(fmt.Sprintf("this is message %v", i))
		request := client.NewRequest("myqueue").WithBody(message)
		reply, err := c.Send(request)

		Equal(t, err, nil)

		getNetConn().Close()

		Equal(t, reply.Body, []byte(fmt.Sprintf("Got message: %s", message)))
	}
}

func testDialer(t *testing.T) (func(string, string) (net.Conn, error), func() net.Conn) {
	var conn net.Conn

	return func(network, addr string) (net.Conn, error) {
			var err error

			conn, err = net.DialTimeout(network, addr, 2*time.Second)
			if err != nil {
				return nil, err
			}
			// Heartbeating hasn't started yet, don't stall forever on a dead server.
			// A deadline is set for TLS and AMQP handshaking. After AMQP is established,
			// the deadline is cleared in openComplete.
			if err = conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				return nil, err
			}

			return conn, nil
		}, func() net.Conn {
			return conn
		}

}
