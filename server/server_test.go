package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"testing"
	"time"

	"github.com/bombsimon/amqp-rpc/logger"
	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

func noEcho() {
	silentLogger := log.New(ioutil.Discard, "", log.LstdFlags)

	logger.SetInfoLogger(silentLogger)
	logger.SetWarnLogger(silentLogger)
}

func replyToConsumer(t *testing.T, ch *amqp.Channel) (<-chan amqp.Delivery, string) {
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	Equal(t, err, nil)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	Equal(t, err, nil)
	return msgs, q.Name
}

func TestPublishReply(t *testing.T) {
	noEcho()

	s := New()
	s.SetTLSConfig(new(tls.Config))

	NotEqual(t, s.dialconfig.TLSClientConfig, nil)

	s.AddHandler("myqueue", func(ctx context.Context, d *amqp.Delivery) []byte {
		return []byte(fmt.Sprintf("Got message: %s", d.Body))
	})

	go s.ListenAndServe("amqp://guest:guest@localhost:5672/")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	Equal(t, err, nil)
	defer conn.Close()

	ch, err := conn.Channel()
	Equal(t, err, nil)
	defer ch.Close()

	msgs, replyQueue := replyToConsumer(t, ch)

	err = ch.Publish(
		"",        // exchange
		"myqueue", // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			ReplyTo:     replyQueue,
			Body:        []byte("this is a message"),
		},
	)
	Equal(t, err, nil)

	reply, ok := <-msgs
	Equal(t, ok, true)
	Equal(t, reply.Body, []byte("Got message: this is a message"))
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

func TestReconnect(t *testing.T) {
	noEcho()

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

	msgs, replyQueue := replyToConsumer(t, ch)

	s := New()
	dialer, getNetConn := testDialer(t)
	s.SetAMQPConfig(amqp.Config{
		Dial: dialer,
	})
	s.AddHandler("myqueue", func(ctx context.Context, d *amqp.Delivery) []byte {
		return []byte(fmt.Sprintf("Got message: %s", d.Body))
	})
	go s.ListenAndServe("amqp://guest:guest@localhost:5672/")

	// Sleep a bit to ensure server is started.
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 2; i++ {
		message := fmt.Sprintf("this is message %v", i)
		err = ch.Publish(
			"",        // exchange
			"myqueue", // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				ReplyTo:     replyQueue,
				Body:        []byte(message),
			},
		)
		Equal(t, err, nil)

		getNetConn().Close()

		select {
		case reply, ok := <-msgs:
			Equal(t, ok, true)
			Equal(t, reply.Body, []byte(fmt.Sprintf("Got message: %s", message)))
		case <-time.After(1100 * time.Millisecond):
			t.Error("reply did not arrive")
		}
	}
}
