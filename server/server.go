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

	// handlers is a list of handlerBinding that holds information about the
	// bindings and handlers.
	handlers []handlerBinding

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
		handlers:    []handlerBinding{},
		middlewares: []MiddlewareFunc{},
		dialconfig: amqp.Config{
			Dial: connection.DefaultDialer,
		},
		exchangeDelcareSettings: connection.ExchangeDeclareSettings{Durable: true},
		queueDeclareSettings:    connection.QueueDeclareSettings{},
		consumeSettings:         connection.ConsumeSettings{},
	}

	return &server
}

type handlerBinding struct {
	exchangeName string
	exchangeType string
	routingKey   string
	bindHeaders  amqp.Table
	handler      HandlerFunc
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
func (s *RPCServer) AddHandler(routingKey string, handler HandlerFunc) {
	s.AddExchangeHandler(routingKey, "", "direct", amqp.Table{}, handler)
}

// AddExchangeHandler will add a handler for any type of exchange and routing
// key. If no values are given for routing key or exchange name the default
// empty string will be used. Custom bind headers are passed as amqp.Table just
// like they are treated when binding the queue.
func (s *RPCServer) AddExchangeHandler(routingKey, exchangeName, exchangeType string, bindHeaders amqp.Table, handler HandlerFunc) {
	opts := handlerBinding{
		exchangeName: exchangeName,
		exchangeType: exchangeType,
		routingKey:   routingKey,
		bindHeaders:  bindHeaders,
		handler:      handler,
	}

	s.handlers = append(s.handlers, opts)
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

	for _, ho := range s.handlers {
		err := s.consume(ho, inputCh)
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

func (s *RPCServer) consume(binding handlerBinding, inputCh *amqp.Channel) error {
	queueName, err := s.declareAndBind(inputCh, binding)
	if err != nil {
		return err
	}

	deliveries, err := inputCh.Consume(
		queueName,
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
	handler := MiddlewareChain(binding.handler, s.middlewares...)

	go s.runHandler(handler, deliveries, queueName)

	return nil
}

func (s *RPCServer) runHandler(handler HandlerFunc, deliveries <-chan amqp.Delivery, queueName string) {
	logger.Infof("server: waiting for messages on queue '%s'", queueName)

	for delivery := range deliveries {
		logger.Infof("server: got delivery on queue %v correlation id %v", queueName, delivery.CorrelationId)

		rw := ResponseWriter{
			publishing: &amqp.Publishing{
				CorrelationId: delivery.CorrelationId,
				Body:          []byte{},
			},
		}

		ctx := context.WithValue(context.Background(), CtxQueueName, queueName)

		handler(ctx, &rw, delivery)
		delivery.Ack(false)

		s.responses <- processedRequest{
			replyTo:    delivery.ReplyTo,
			mandatory:  rw.mandatory,
			immediate:  rw.immediate,
			publishing: *rw.publishing,
		}
	}

	logger.Infof("server: stopped waiting for messages on queue '%s'", queueName)
}

func (s *RPCServer) declareAndBind(inputCh *amqp.Channel, binding handlerBinding) (string, error) {
	queue, err := inputCh.QueueDeclare(
		binding.routingKey,
		s.queueDeclareSettings.Durable,
		s.queueDeclareSettings.DeleteWhenUnused,
		s.queueDeclareSettings.Exclusive,
		s.queueDeclareSettings.NoWait,
		s.queueDeclareSettings.Args,
	)

	if err != nil {
		return "", err
	}

	if binding.exchangeName == "" {
		return queue.Name, nil
	}

	err = inputCh.ExchangeDeclare(
		binding.exchangeName,
		binding.exchangeType,
		s.exchangeDelcareSettings.Durable,
		s.exchangeDelcareSettings.AutoDelete,
		s.exchangeDelcareSettings.Internal,
		s.exchangeDelcareSettings.NoWait,
		s.exchangeDelcareSettings.Args,
	)

	if err != nil {
		return "", err
	}

	err = inputCh.QueueBind(
		queue.Name,
		binding.routingKey,
		binding.exchangeName,
		s.queueDeclareSettings.NoWait, // Use same value as for declaring.
		binding.bindHeaders,
	)

	if err != nil {
		return "", err
	}

	return queue.Name, nil
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
