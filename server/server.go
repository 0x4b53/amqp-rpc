package server

import (
	"context"
	"errors"
	"fmt"
	"os"

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
	handlers  map[string]handlerFunc
	responses chan responseObj
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
func (s *RPCServer) ListenAndServe() {
	for {
		err := s.listenAndServe()
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (s *RPCServer) listenAndServe() error {
	s.responses = make(chan responseObj)

	conn, err := amqp.Dial(os.Getenv("AMQP_URL"))
	if err != nil {
		return err
	}

	monitorConnection(conn)

	inputCh, err := conn.Channel()
	if err != nil {
		return err
	}

	for queueName, handler := range s.handlers {
		err := s.consume(queueName, handler, inputCh)
		if err != nil {
			return err
		}
	}

	return nil
}

func monitorConnection(c *amqp.Connection) {
	go func() {
		for {
			connClosed := c.NotifyClose(make(chan *amqp.Error))

			if <-connClosed != nil {
				// Time to reconnect
			}
		}
	}()
}

func (s *RPCServer) responder(outCh *amqp.Channel) error {
	for {
		response, ok := <-s.responses
		if !ok {
			return ErrResponseChClosed
		}

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
			return ErrChannelClosed
		}
	}
}

func (s *RPCServer) consume(queueName string, handler handlerFunc, inputCh *amqp.Channel) error {
	deliveries, err := inputCh.Consume(
		queueName, // queue
		"",        // consumer
		false,     // autoAck
		false,     // exclusive
		false,     // noLocal
		false,     // noWait
		nil,       // args
	)
	if err != nil {
		return err
	}

	for {
		delivery, ok := <-deliveries
		if !ok {
			return ErrChannelClosed
		}

		go func(d *amqp.Delivery) {
			response := handler(context.TODO(), d)

			s.responses <- responseObj{
				response: response,
				delivery: d,
			}
		}(&delivery)
	}
}
