package server

import (
	"context"
	"errors"
	"time"

	"github.com/streadway/amqp"

	"github.com/bombsimon/amqp-rpc/connection"
	"github.com/bombsimon/amqp-rpc/logger"
	"github.com/bombsimon/amqp-rpc/middleware"
)

var (
	// ErrResponseChClosed is an error representing a closed response channel.
	ErrResponseChClosed = errors.New("Channel closed")
)

// handlerFunc is the function that handles all request based on the routing key.
type handlerFunc func(context.Context, amqp.Delivery) []byte

// RPCServer represents an AMQP server used within the RPC framework.
// The server uses handlers to map a routing key to a handler function.
type RPCServer struct {
	// url is the URL where the server should dial to start subscribing.
	url string

	// handlers is a map where the routing key is used as map key
	// and the value is a function of type handlerFunc.
	handlers map[string]handlerFunc

	// middlewares is a list of functions which will be executed
	// before calling the handler for a specific endpoint.
	middlewares []middleware.ServerMiddleware

	// Every processed request will be responded to in a separate
	// go routine. The server holds a chanel on which all the responses
	// from a handler func is added.
	responses chan processedRequest

	// dialconfig is a amqp.Config which holds information about the connection
	// such as authentication, TLS configuration, and a dailer which is a
	// function used to obtain a connection.
	// By default the dialconfig will include a dail function implemented in
	// connection/dialer.go.
	dialconfig amqp.Config

	// queueDeclareSettings is configuration used when declaring a RabbitMQ queue.
	queueDeclareSettings connection.QueueDeclareSettings

	// consumeSetting is configuration used when consuming from the message bus.
	consumeSettings connection.ConsumeSettings

	// publishSettings is the configuration used when publishing a message with the client
	publishSettings connection.PublishSettings
}

// processedRequest is used to add the response from a handler func combined
// with a amqp.Delivery. The reasone we need to combine those is that we
// reply to each request in a separate go routine and the delivery is required
// to determine on which queue to reply.
type processedRequest struct {
	delivery amqp.Delivery
	response []byte
}

// New will return a pointer to a new RPCServer.
func New(url string) *RPCServer {
	server := RPCServer{
		url:         url,
		handlers:    map[string]handlerFunc{},
		middlewares: []middleware.ServerMiddleware{},
		dialconfig:  amqp.Config{
			// Dial: connection.DefaultDialer,
		},
		queueDeclareSettings: connection.QueueDeclareSettings{},
		consumeSettings:      connection.ConsumeSettings{},
		publishSettings:      connection.PublishSettings{},
	}

	return &server
}

func Close() {

}

// WithDialConfig sets the dial config used for the server.
func (s *RPCServer) WithDialConfig(c amqp.Config) *RPCServer {
	s.dialconfig = c

	return s
}

// AddMiddleware will add a ServerMiddleware to the list of middlewares to be
// triggered before the handle func for each request.
func (s *RPCServer) AddMiddleware(m middleware.ServerMiddleware) *RPCServer {
	s.middlewares = append(s.middlewares, m)

	return s
}

// AddHandler adds a new handler to the RPC server.
func (s *RPCServer) AddHandler(queueName string, handler handlerFunc) {
	s.handlers[queueName] = handler
}

// ListenAndServe will dial the RabbitMQ message bus, set up
// all the channels, consume from all RPC server queues and monitor
// to connection to ensure the server is always connected.
func (s *RPCServer) ListenAndServe() {
	s.responses = make(chan processedRequest)

	for {
		err := s.listenAndServe()
		if err != nil {
			logger.Warnf("server: got error: %s, will reconnect in %d second(s)", err, 0.5)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		logger.Info("server: listener exiting gracefully")
		break
	}
}

func (s *RPCServer) listenAndServe() error {
	logger.Infof("server: staring listener: %s", s.url)

	conn, err := amqp.DialConfig(s.url, s.dialconfig)
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
		queueName,
		s.queueDeclareSettings.Durable,
		s.queueDeclareSettings.DeleteWhenUnused,
		s.queueDeclareSettings.Exclusive,
		s.queueDeclareSettings.NoWait,
		s.queueDeclareSettings.Args,
	)

	if err != nil {
		return err
	}

	deliveries, err := inputCh.Consume(
		queue.Name,
		s.consumeSettings.Consumer,
		s.consumeSettings.AutoAck,
		s.consumeSettings.Exclusive,
		s.consumeSettings.NoLocal,
		s.consumeSettings.NoWait,
		s.consumeSettings.Args,
	)

	if err != nil {
		return err
	}

	go func() {
		logger.Infof("server: waiting for messages on queue '%s'", queue.Name)

		for delivery := range deliveries {
			logger.Infof("server: got delivery on queue %v correlation id %v", queue.Name, delivery.CorrelationId)
			var (
				errMiddleware error
				response      []byte
			)

			for _, middleware := range s.middlewares {
				errMiddleware = middleware(queue.Name, context.TODO(), &delivery)
				if errMiddleware != nil {
					response = []byte(errMiddleware.Error())
					break
				}
			}

			if errMiddleware == nil {
				response = handler(context.TODO(), delivery)
			}

			delivery.Ack(false)

			s.responses <- processedRequest{
				response: response,
				delivery: delivery,
			}
		}

		logger.Infof("server: stopped waiting for messages on queue '%s'", queue.Name)
	}()

	return nil
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
			s.publishSettings.Mandatory,
			s.publishSettings.Immediate,
			amqp.Publishing{
				Body:          response.response,
				CorrelationId: response.delivery.CorrelationId,
			},
		)

		if err != nil {
			logger.Warnf("server: could not publish response, will retry later")
			s.responses <- response
			return err
		}

		logger.Infof(
			"server: successfully published response %v to %v",
			response.delivery.CorrelationId,
			response.delivery.ReplyTo,
		)
	}
}
