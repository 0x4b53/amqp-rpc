package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

var (
	// ErrChannelClosed is an error representing a closed channel.
	ErrChannelClosed = errors.New("Channel closed")
	// ErrResponseChClosed is an error representing a closed response channel.
	ErrResponseChClosed = errors.New("Channel closed")
)

// RPCServer represents a RabbitMQ RPC server.
// The server holds a map of all handlers.
type RPCServer struct {
	handlers    map[string]handlerFunc
	responses   chan responseObj
	currentConn *amqp.Connection
}

type responseObj struct {
	delivery *amqp.Delivery
	response []byte
}

type handlerFunc func(context.Context, *amqp.Delivery) []byte

// New will return a pointer to a new RPCServer.
func New() *RPCServer {
	return &RPCServer{
		handlers: map[string]handlerFunc{},
	}
}

// AddHandler adds a new handler to the RPC server.
func (s *RPCServer) AddHandler(queueName string, handler handlerFunc) {
	s.handlers[queueName] = handler
}

// ListenAndServe will dial the RabbitMQ message bus, set up
// all the channels, consume from all RPC server queues and monitor
// to connection to ensure the server is always connected.
func (s *RPCServer) ListenAndServe(url string) {
	s.responses = make(chan responseObj)

	for {
		err := s.listenAndServe(url)
		fmt.Println("Exited listenAndServe")
		if err != nil {
			fmt.Println("Got error:", err, "will reconnect in 1 second")
		}
		fmt.Println("Reconnecting 1 second")

		time.Sleep(1 * time.Second)
	}
}

func (s *RPCServer) listenAndServe(url string) error {
	fmt.Println("starting listener:", url)

	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}

	s.currentConn = conn

	defer conn.Close()

	inputCh, err := conn.Channel()
	if err != nil {
		return err
	}
	defer inputCh.Close()

	for queueName, handler := range s.handlers {
		err := s.consume(queueName, handler, inputCh)
		if err != nil {
			return err
		}
	}

	outputCh, err := conn.Channel()
	if err != nil {
		return err
	}
	defer outputCh.Close()

	go s.responder(outputCh)

	return <-conn.NotifyClose(make(chan *amqp.Error))
}

func (s *RPCServer) consume(queueName string, handler handlerFunc, inputCh *amqp.Channel) error {
	queue, err := inputCh.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		return err
	}

	deliveries, err := inputCh.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // autoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // args
	)

	if err != nil {
		return err
	}

	fmt.Println("Waiting for messages on queue", queue.Name)
	go func() {
		for delivery := range deliveries {
			fmt.Println("Got RPC delivery", string(delivery.Body))
			response := handler(context.TODO(), &delivery)

			s.responses <- responseObj{
				response: response,
				delivery: &delivery,
			}
		}
	}()

	return nil
}

func (s *RPCServer) responder(outCh *amqp.Channel) error {
	for {
		response, ok := <-s.responses
		if !ok {
			return ErrResponseChClosed
		}

		fmt.Println("Will publish response", string(response.response))
		err := outCh.Publish(
			"", // exchange
			response.delivery.ReplyTo,
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				Body: response.response,
			},
		)

		if err != nil {
			fmt.Println("Could not publish response, will retry later")
			s.responses <- response
			return err
		}
	}
}
