package server

import (
	"context"
	"errors"
	"time"

	"github.com/streadway/amqp"

	"github.com/bombsimon/amqp-rpc/connection"
	"github.com/bombsimon/amqp-rpc/logger"
)

type ctxKey string

const (
	// CtxQueueName can be used to get the queue name from the context.Context
	// inside the HandlerFunc.
	CtxQueueName ctxKey = "queue_name"
)

var (
	// ErrResponseChClosed is an error representing a closed response channel.
	ErrResponseChClosed = errors.New("Channel closed")
)

// HandlerFunc is the function that handles all request based on the routing key.
type HandlerFunc func(context.Context, *ResponseWriter, amqp.Delivery)

// processedRequest is used to add the response from a handler func combined
// with a amqp.Delivery. The reasone we need to combine those is that we reply
// to each request in a separate go routine and the delivery is required to
// determine on which queue to reply.
type processedRequest struct {
	replyTo    string
	mandatory  bool
	immediate  bool
	publishing amqp.Publishing
}

// RPCServer represents an AMQP server used within the RPC framework. The
// server uses handlers to map a routing key to a handler function.
type RPCServer struct {
	// url is the URL where the server should dial to start subscribing.
	url string

	// handlers is a map where the routing key is used as map key and the value
	// is a function of type HandlerFunc.
	handlers map[string]HandlerFunc

	// fanoutHandlers is a map where the exchange is used as a map key and the
	// value is a function of type HandleFunc.
	fanoutHandlers map[string]HandlerFunc

	// middlewares are chained and executed on request.
	middlewares []MiddlewareFunc

	// Every processed request will be responded to in a separate go routine.
	// The server holds a chanel on which all the responses from a handler func
	// is added.
	responses chan processedRequest

	// dialconfig is a amqp.Config which holds information about the connection
	// such as authentication, TLS configuration, and a dailer which is a
	// function used to obtain a connection. By default the dialconfig will
	// include a dail function implemented in connection/dialer.go.
	dialconfig amqp.Config

	// exchangeDelcareSettings is configurations used when declaring a RabbitMQ
	// exchange.
	exchangeDelcareSettings connection.ExchangeDeclareSettings

	// queueDeclareSettings is configuration used when declaring a RabbitMQ
	// queue.
	queueDeclareSettings connection.QueueDeclareSettings

	// consumeSetting is configuration used when consuming from the message
	// bus.
	consumeSettings connection.ConsumeSettings
}

// New will return a pointer to a new RPCServer.
func New(url string) *RPCServer {
	server := RPCServer{
		url:         url,
		handlers:    map[string]HandlerFunc{},
		middlewares: []MiddlewareFunc{},
		dialconfig: amqp.Config{
			Dial: connection.DefaultDialer,
		},
		exchangeDelcareSettings: connection.ExchangeDeclareSettings{
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
		},
		queueDeclareSettings: connection.QueueDeclareSettings{},
		consumeSettings:      connection.ConsumeSettings{},
	}

	return &server
}

// WithDialConfig sets the dial config used for the server.
func (s *RPCServer) WithDialConfig(c amqp.Config) *RPCServer {
	s.dialconfig = c

	return s
}

// AddMiddleware will add a ServerMiddleware to the list of middlewares to be
// triggered before the handle func for each request.
func (s *RPCServer) AddMiddleware(m MiddlewareFunc) *RPCServer {
	s.middlewares = append(s.middlewares, m)

	return s
}

// AddHandler adds a new handler to the RPC server.
func (s *RPCServer) AddHandler(queueName string, handler HandlerFunc) {
	s.handlers[queueName] = handler
}

// AddFanoutHandler add a new handler to the RPC server wihch will work as a
// fanout handler on a given exchange.
func (s *RPCServer) AddFanoutHandler(exchange string, handler HandlerFunc) {
	s.fanoutHandlers[exchange] = handler
}

// ListenAndServe will dial the RabbitMQ message bus, set up all the channels,
// consume from all RPC server queues and monitor to connection to ensure the
// server is always connected.
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
		err := s.consume(queueName, "", handler, inputCh)
		if err != nil {
			return err
		}
	}

	for exchangeName, handler := range s.fanoutHandlers {
		err := s.consume("", exchangeName, handler, inputCh)
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

func (s *RPCServer) consume(queueName, exchangeName string, handler HandlerFunc, inputCh *amqp.Channel) error {
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

	if exchangeName != "" {
		err := inputCh.ExchangeDeclare(
			exchangeName,
			"fanout",
			s.exchangeDelcareSettings.Durable,
			s.exchangeDelcareSettings.AutoDelete,
			s.exchangeDelcareSettings.Internal,
			s.exchangeDelcareSettings.NoWait,
			s.exchangeDelcareSettings.Args,
		)

		if err != nil {
			return err
		}

		err = inputCh.QueueBind(
			queue.Name,
			queueName,
			exchangeName,
			false,
			nil,
		)

		if err != nil {
			return err
		}
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

	// Attach the middlewares to the handler.
	handler = MiddlewareChain(handler, s.middlewares...)

	go func() {
		logger.Infof("server: waiting for messages on queue '%s'", queue.Name)

		for delivery := range deliveries {
			logger.Infof("server: got delivery on queue %v correlation id %v", queue.Name, delivery.CorrelationId)

			rw := ResponseWriter{
				publishing: &amqp.Publishing{
					CorrelationId: delivery.CorrelationId,
					Body:          []byte{},
				},
			}

			ctx := context.WithValue(context.Background(), CtxQueueName, queue.Name)

			handler(ctx, &rw, delivery)
			delivery.Ack(false)

			s.responses <- processedRequest{
				replyTo:    delivery.ReplyTo,
				mandatory:  rw.mandatory,
				immediate:  rw.immediate,
				publishing: *rw.publishing,
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
			response.replyTo,
			response.mandatory,
			response.immediate,
			response.publishing,
		)

		if err != nil {
			logger.Warnf("server: could not publish response, will retry later")
			s.responses <- response
			return err
		}

		logger.Infof(
			"server: successfully published response %v to %v",
			response.publishing.CorrelationId,
			response.replyTo,
		)
	}
}
