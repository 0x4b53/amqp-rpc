package amqprpc

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// The default exchange types that are available in RabbitMQ.
const (
	ExchangeTypeDirect  = "direct"
	ExchangeTypeTopic   = "topic"
	ExchangeTypeHeaders = "headers"
)

// The default exchanges that are available in RabbitMQ.
const (
	DefaultExchangeNameDirect  = "amq.direct"
	DefaultExchangeNameTopic   = "amq.topic"
	DefaultExchangeNameHeaders = "amq.match"
)

// The different queue types that are available in RabbitMQ.
const (
	QueueTypeClassic = "classic"
	QueueTypeQuorum  = "quorum"
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
	// Setting setting it to false not work with quorum queues.
	QueueDurable bool

	// Set the queue to be automatically deleted when the last consumer stops.
	QueueAutoDelete bool

	// SkipQueueDeclare will skip the queue declaration. This can be useful when
	// you are migrating queue types or arguments and want to avoid the
	// PRECONDITION_FAILED error.
	SkipQueueDeclare bool

	// QueueExclusive sets the exclusive flag when declaring the queue.
	// Exclusive queues can only be used by the connection that created them.
	// If exclusive is true, the queue will be deleted during a network failure
	// making incoming requests fail before the reconnect and redeclare
	// happens.
	QueueExclusive bool

	// QueueDeclareArgs sets any extra queue arguments.
	QueueDeclareArgs amqp.Table

	// AutoAck sets the auto-ack flag on the consumer.
	AutoAck bool

	// PrefetchCount sets the prefetch count for the consumer. This is only
	// really usable when AutoAck is also set to false.
	PrefetchCount int

	// ExclusiveConsumer sets the exclusive flag when starting the consumer.
	// This ensures that the bound handler is the only consumer of its queue.
	// This works only with classic queues and setting this to true while using
	// a quorum queue will silently allow multiple consumers.
	ExclusiveConsumer bool

	// ConsumerArgs sets any extra consumer arguments.
	ConsumerArgs amqp.Table
}

// WithQueueName sets the name of the queue that the handler should be bound to.
func (b HandlerBinding) WithQueueName(name string) HandlerBinding {
	b.QueueName = name
	return b
}

// WithExchangeName sets the exchange that the queue should be bound to.
func (b HandlerBinding) WithExchangeName(name string) HandlerBinding {
	b.ExchangeName = name
	return b
}

// WithRoutingKey sets the routing key that the queue should be bound to.
func (b HandlerBinding) WithRoutingKey(key string) HandlerBinding {
	b.RoutingKey = key
	return b
}

// WithBindHeaders sets the headers that the queue should be bound to.
func (b HandlerBinding) WithBindHeaders(headers amqp.Table) HandlerBinding {
	b.BindHeaders = headers
	return b
}

// WithHandler sets the function that should be called when a message is received.
func (b HandlerBinding) WithHandler(handler HandlerFunc) HandlerBinding {
	b.Handler = handler
	return b
}

// WithQueueDurable sets the durable flag for the queue.
func (b HandlerBinding) WithQueueDurable(durable bool) HandlerBinding {
	b.QueueDurable = durable
	return b
}

// WithQueueAutoDelete sets the queue to be automatically deleted when the last consumer stops.
func (b HandlerBinding) WithQueueAutoDelete(autoDelete bool) HandlerBinding {
	b.QueueAutoDelete = autoDelete
	return b
}

// WithSkipQueueDeclare sets the flag to skip the queue declaration.
func (b HandlerBinding) WithSkipQueueDeclare(skip bool) HandlerBinding {
	b.SkipQueueDeclare = skip
	return b
}

// WithQueueExclusive sets the exclusive flag when declaring the queue.
// Exclusive queues can only be used by the connection that created them. If
// exclusive is true, the queue will be deleted during a network failure making
// incoming requests fail before the reconnect and redeclare happens.
func (b HandlerBinding) WithQueueExclusive(exclusive bool) HandlerBinding {
	b.QueueExclusive = exclusive
	return b
}

// WithQueueDeclareArgs sets extra queue arguments.
func (b HandlerBinding) WithQueueDeclareArgs(args amqp.Table) HandlerBinding {
	b.QueueDeclareArgs = args
	return b
}

// WithQueueDeclareArg sets one queue argument, this ensures that the queue
// default arguments are not overwritten.
func (b HandlerBinding) WithQueueDeclareArg(key string, val any) HandlerBinding {
	b.QueueDeclareArgs[key] = val
	return b
}

// WithAutoAck sets the auto-ack flag on the consumer.
func (b HandlerBinding) WithAutoAck(autoAck bool) HandlerBinding {
	b.AutoAck = autoAck
	return b
}

// WithPrefetchCount sets the prefetch count for the consumer.
func (b HandlerBinding) WithPrefetchCount(count int) HandlerBinding {
	b.PrefetchCount = count
	return b
}

// WithExclusiveConsumer sets the exclusive flag when starting the consumer.
// This ensures that the bound handler is the only consumer of its queue. This
// works only with classic queues and setting this to true while using a quorum
// queue will silently allow multiple consumers.
func (b HandlerBinding) WithExclusiveConsumer(exclusive bool) HandlerBinding {
	b.ExclusiveConsumer = exclusive
	return b
}

// WithConsumerArgs sets any extra consumer arguments.
func (b HandlerBinding) WithConsumerArgs(args amqp.Table) HandlerBinding {
	b.ConsumerArgs = args
	return b
}

// WithConsumerArg sets one extra consumer argument, this ensures that the
// consumer default arguments are not overwritten.
func (b HandlerBinding) WithConsumerArg(key string, val any) HandlerBinding {
	b.ConsumerArgs[key] = val
	return b
}

// CreateBinding returns a HandlerBinding with default values set.
func CreateBinding(queueName, exchangeName string, handler HandlerFunc) HandlerBinding {
	return HandlerBinding{
		QueueName:    queueName,
		ExchangeName: exchangeName,
		BindHeaders:  amqp.Table{},
		Handler:      handler,
		// Must be true when using a quorum queue. And is a good default when
		// using a cluster.
		QueueDurable: true,
		QueueDeclareArgs: amqp.Table{
			// This is a good default queue for modern rabbitmq
			// installations.
			"x-queue-type": QueueTypeQuorum,
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
	// The queue name is the same as the routing key in a direct binding.
	return CreateBinding(routingKey, DefaultExchangeNameDirect, handler).
		WithRoutingKey(routingKey)
}

// TopicBinding returns a HandlerBinding to use for topic exchanges. The default
// exchange (amq.topic) will be used. The topic is matched on the routing key.
func TopicBinding(queueName, routingKey string, handler HandlerFunc) HandlerBinding {
	return CreateBinding(queueName, DefaultExchangeNameTopic, handler).
		WithRoutingKey(routingKey)
}

// HeadersBinding returns a HandlerBinding to use for header exchanges that
// will match on specific headers. The headers are specified as an amqp.Table.
// The default exchange amq.match will be used.
func HeadersBinding(queueName string, headers amqp.Table, handler HandlerFunc) HandlerBinding {
	return CreateBinding(queueName, DefaultExchangeNameHeaders, handler).
		WithBindHeaders(headers)
}

// ExchangeDeclareSettings is the settings that will be used when a handler
// is mapped to a fanout exchange and an exchange is declared.
type ExchangeDeclareSettings struct {
	// Name is the name of the exchange.
	Name string

	// Type is the exchange type.
	Type string

	// Durable sets the durable flag. Durable exchanges survives server restart.
	Durable bool

	// AutoDelete sets the auto-delete flag, this ensures the exchange is
	// deleted when it isn't bound to any more.
	AutoDelete bool

	// Args sets the arguments table used.
	Args amqp.Table
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
