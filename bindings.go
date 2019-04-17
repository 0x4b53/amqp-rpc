package amqprpc

import (
	"github.com/streadway/amqp"
)

// Exchanges are enteties where messages are sent. This defines the available
// entities based on https://www.rabbitmq.com/tutorials/amqp-concepts.html.
const (
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

// HandlerBinding holds information about how an exchange and a queue should be
// declared and bound. If the ExchangeName is not defined (an empty string), the
// queue will not be bound to the exchange but assumed to use the default match.
type HandlerBinding struct {
	QueueName    string
	ExchangeName string
	ExchangeType string
	RoutingKey   string
	BindHeaders  amqp.Table
	Handler      HandlerFunc
	WorkerCount  uint
}

// WithWorkerCount sets the maximum concurrent workers for a handler.
func (hb HandlerBinding) WithWorkerCount(c uint) HandlerBinding {
	hb.WorkerCount = c
	return hb
}

// DirectBinding returns a HandlerBinding to use for direct exchanges where each
// routing key will be mapped to one handler.
func DirectBinding(routingKey string, handler HandlerFunc) HandlerBinding {
	return HandlerBinding{
		QueueName:    routingKey,
		ExchangeName: "amq.direct",
		ExchangeType: ExchangeDirect,
		RoutingKey:   routingKey,
		BindHeaders:  amqp.Table{},
		Handler:      handler,
	}
}

// FanoutBinding returns a HandlerBinding to use for fanout exchanges. These
// exchanges does not use the routing key. We do not use the default exchange
// (amq.fanout) since this would broadcast all messages everywhere.
func FanoutBinding(exchangeName string, handler HandlerFunc) HandlerBinding {
	return HandlerBinding{
		ExchangeName: exchangeName,
		ExchangeType: ExchangeFanout,
		RoutingKey:   "",
		BindHeaders:  amqp.Table{},
		Handler:      handler,
	}
}

// TopicBinding returns a HandlerBinding to use for topic exchanges. The default
// exchange (amq.topic) will be used. The topic is matched on the routing key.
func TopicBinding(queueName, routingKey string, handler HandlerFunc) HandlerBinding {
	return HandlerBinding{
		QueueName:    queueName,
		ExchangeName: "amq.topic",
		ExchangeType: ExchangeTopic,
		RoutingKey:   routingKey,
		BindHeaders:  amqp.Table{},
		Handler:      handler,
	}
}

// HeadersBinding returns a HandlerBinding to use for header exchanges that will
// match on specific headers. The heades are specified as an amqp.Table. The
// default exchange amq.match will be used.
func HeadersBinding(queueName string, headers amqp.Table, handler HandlerFunc) HandlerBinding {
	return HandlerBinding{
		QueueName:    queueName,
		ExchangeName: "amq.match",
		ExchangeType: ExchangeHeaders,
		RoutingKey:   "",
		BindHeaders:  headers,
		Handler:      handler,
	}
}
