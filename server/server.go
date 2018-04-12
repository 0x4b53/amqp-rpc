package server

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/streadway/amqp"

	"github.com/bombsimon/amqp-rpc/logger"
)

var (
	// ErrResponseChClosed is an error representing a closed response channel.
	ErrResponseChClosed = errors.New("Channel closed")
)

// RPCServer represents a RabbitMQ RPC server.
// The server holds a map of all handlers.
type RPCServer struct {
	handlers   map[string]handlerFunc
	responses  chan responseObj
	dialconfig amqp.Config
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

// SetAMQPConfig sets the amqp.Config object to be used when dialing.
func (s *RPCServer) SetAMQPConfig(config amqp.Config) {
	s.dialconfig = config
}

// SetTLSConfig sets the tls.Config on the AMQP config field TLSClientConfig
func (s *RPCServer) SetTLSConfig(c *tls.Config) {
	s.dialconfig.TLSClientConfig = c
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
		if err != nil {
			logger.Warnf("got error: %s, will reconnect in %d second(s)", err, 1)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		logger.Info("listener exiting gracefully")
		break
	}
}

func (s *RPCServer) listenAndServe(url string) error {
	logger.Infof("staring listener: %s", url)

	conn, err := amqp.DialConfig(url, s.dialconfig)
	if err != nil {
		return err
	}

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

	err, ok := <-conn.NotifyClose(make(chan *amqp.Error))
	if !ok {
		// The connection was closed gracefully.
		return nil
	}
	// The connection wasn't closed gracefully.
	return err
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
		false,      // autoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // args
	)

	if err != nil {
		return err
	}

	go func() {
		logger.Infof("waiting for messages on queue '%s'", queue.Name)
		for delivery := range deliveries {
			logger.Info("got message")
			response := handler(context.TODO(), &delivery)
			delivery.Ack(false)

			s.responses <- responseObj{
				response: response,
				delivery: &delivery,
			}
		}
		logger.Infof("stopped waiting for messages on queue '%s'", queue.Name)
	}()

	return nil
}

func (s *RPCServer) responder(outCh *amqp.Channel) error {
	for {
		response, ok := <-s.responses
		if !ok {
			return ErrResponseChClosed
		}

		logger.Infof("request processed, will publish response")
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
			logger.Warnf("could not publish response, will retry later")
			s.responses <- response
			return err
		}
	}
}
