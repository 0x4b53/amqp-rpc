package amqprpc

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// Exchanges are entities where messages are published. This defines the available
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
	// QueueName is the name of the queue that the handler should be bound to.
	QueueName string

	// ExchangeName is the exchange that the queue should be bound to.
	ExchangeName string

	// RoutingKey is the routing key that the queue should be bound to.
	RoutingKey string

	// BindHeaders is the headers that the queue should be bound to.
	BindHeaders amqp.Table

	// Handler is the function that should be called when a message is
	// received.
	Handler HandlerFunc

	// QueueDurable sets the durable flag. Should not be used together with
	// QueueExclusive since that will fail in a future RabbitMQ version.
	QueueDurable bool

	// Set the queue to be automatically deleted when the last consumer stops.
	QueueAutoDelete bool

	// SkipQueueDeclare will skip the queue declaration. You can be useful when
	// you are migrating queue types or arguments and want to avoid the
	// PRECONDITION_FAILED error. If this is set to true, you cannot use auto
	// generated queue names.
	SkipQueueDeclare bool

	// Exclusive queues can only be used by the connection that created them.
	// If this is true, the queue will be deleted during a network failure
	// making any requests fail before the reconnect can happen.
	QueueExclusive bool

	// QueueArgs sets any extra queue arguments.
	QueueArgs amqp.Table

	// AutoAck sets the auto-ack flag on the consumer.
	AutoAck bool

	// PrefetchCount sets the prefetch count for the consumer. This is only
	// really usable when AutoAck is also set to false.
	PrefetchCount int

	// ConsumerArgs sets any extra consumer arguments.
	ConsumerArgs amqp.Table
}

// With will apply the given functions to the HandlerBinding. This is a
// convenient way of setting options.
func (b HandlerBinding) With(funcs ...func(*HandlerBinding)) HandlerBinding {
	for _, f := range funcs {
		f(&b)
	}

	return b
}

// CreateBinding returns a HandlerBinding with default values set.
func CreateBinding(handler HandlerFunc) HandlerBinding {
	return HandlerBinding{
		BindHeaders: amqp.Table{},
		Handler:     handler,
		// Must be true when using a quorum queue. And is a good default when
		// using a cluster.
		QueueDurable: true,
		QueueArgs: amqp.Table{
			// This is a good default queue for modern rabbitmq
			// installations.
			"x-queue-type": "quorum",
			// Remove queues not used for 30 minutes. This is a good
			// default instead of using the auto-delete flag since it gives
			// the handler time to reconnect in case of a failure.
			"x-expires": 30 * 60 * 1000,
		},
		// Default to auto ack.
		AutoAck: true,
		// Use a reasonable default value.
		// https://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
		// https://godoc.org/github.com/rabbitmq/amqp091-go#Channel.Qos
		// In reality, this makes no difference unless AutoAck is set to false.
		PrefetchCount: 10,
		ConsumerArgs:  amqp.Table{},
	}
}

// DirectBinding returns a HandlerBinding to use for direct exchanges where each
// routing key will be mapped to one handler.
func DirectBinding(routingKey string, handler HandlerFunc) HandlerBinding {
	binding := CreateBinding(handler)
	binding.QueueName = routingKey
	binding.RoutingKey = routingKey
	binding.ExchangeName = "amq.direct"

	return binding
}

// TopicBinding returns a HandlerBinding to use for topic exchanges. The default
// exchange (amq.topic) will be used. The topic is matched on the routing key.
func TopicBinding(queueName, routingKey string, handler HandlerFunc) HandlerBinding {
	binding := CreateBinding(handler)
	binding.QueueName = queueName
	binding.RoutingKey = routingKey
	binding.ExchangeName = "amq.topic"

	return binding
}

// HeadersBinding returns a HandlerBinding to use for header exchanges that
// will match on specific headers. The headers are specified as an amqp.Table.
// The default exchange amq.match will be used.
func HeadersBinding(queueName string, headers amqp.Table, handler HandlerFunc) HandlerBinding {
	binding := CreateBinding(handler)
	binding.QueueName = queueName
	binding.ExchangeName = "amq.match"
	binding.BindHeaders = headers

	return binding
}

func createExchanges(ch *amqp.Channel, exchanges []ExchangeDeclareSettings) error {
	for _, e := range exchanges {
		err := ch.ExchangeDeclare(
			e.Name,
			e.Type,
			e.Durable,
			e.AutoDelete,
			false, // internal.
			false, // no-wait.
			e.Args,
		)
		if err != nil {
			return err
		}
	}

	return nil
}
