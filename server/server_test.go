package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

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
	s := New()

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

	s := New()
	s.AddHandler("myqueue", func(ctx context.Context, d *amqp.Delivery) []byte {
		return []byte(fmt.Sprintf("Got message: %s", d.Body))
	})
	go s.ListenAndServe("amqp://guest:guest@localhost:5672/")

	msgs, replyQueue := replyToConsumer(t, ch)

	time.Sleep(50 * time.Millisecond)
	s.currentConn.Close()
	time.Sleep(50 * time.Millisecond)

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
