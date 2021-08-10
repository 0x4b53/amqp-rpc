package amqprpc

import (
	"context"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

type wrappedConsumer struct {
	definintion ConsumerDefinition
	messages    chan amqp.Delivery
}

type ConsumerDefinition struct {
	QueueName string
	Tag       string
	AutoAck   bool
	Exclusive bool
	Args      amqp.Table

	RoutingKey  string
	BindHeaders amqp.Table

	// The exchange name and type to bind to. If the exchange doesn't exist, it
	// will be created.
	ExchangeName string
	ExchangeType string

	// When both the queue and the exchange is durable, the binding will
	// presist after a server restart.
	DurableQueue    bool
	DurableExchange bool

	AutoDeleteQueue    bool
	AutoDeleteExchange bool

	// Queue and Exchange extra creation arguments.
	QueueArgs    amqp.Table
	ExchangeArgs amqp.Table
}

type ConsumerMultiplexer struct {
	url        string
	dialConfig amqp.Config

	didStop chan struct{}
	stop    chan struct{}

	wg        sync.WaitGroup
	mu        sync.Mutex
	consumers map[string]wrappedConsumer
	ch        *amqp.Channel
}

func NewConsumerMultiplexer(url string) *ConsumerMultiplexer {
	return &ConsumerMultiplexer{
		url:       url,
		consumers: map[string]wrappedConsumer{},
	}
}

func (c *ConsumerMultiplexer) WithDialConfig(conf amqp.Config) *ConsumerMultiplexer {
	c.dialConfig = conf
	return c
}

func (c *ConsumerMultiplexer) Start(ctx context.Context) {
	once := sync.Once{}
	started := make(chan struct{})

	c.didStop = make(chan struct{})
	c.stop = make(chan struct{})

	go autoReconnect(c.didStop, c.stop, func() error {
		conn, err := amqp.DialConfig(c.url, c.dialConfig)
		if err != nil {
			return err
		}

		defer conn.Close()

		ch, err := conn.Channel()
		if err != nil {
			return err
		}

		defer ch.Close()

		c.ch = ch

		// Initialize all pre-configured consumers.
		for _, wrapped := range c.consumers {
			err := c.startConsumer(wrapped)
			if err != nil {
				return err
			}
		}

		once.Do(func() {
			close(started)
		})

		fmt.Println("starting monitor")
		err = monitorAndWait(
			ctx.Done(),
			ch.NotifyClose(make(chan *amqp.Error)),
			conn.NotifyClose(make(chan *amqp.Error)),
		)
		if err != nil {
			c.wg.Wait()
			return err
		}

		// Clean exit.
		for _, wrapped := range c.consumers {
			err = ch.Cancel(wrapped.definintion.Tag, true)
		}

		c.wg.Wait()

		return nil
	})

	<-started
}

func (c *ConsumerMultiplexer) createTopology(def ConsumerDefinition) error {
	err := c.ch.ExchangeDeclare(
		def.ExchangeName,
		def.ExchangeType,
		def.DurableExchange,
		def.AutoDeleteExchange,
		false,
		false,
		def.ExchangeArgs,
	)
	if err != nil {
		return fmt.Errorf("could not declare exchange: %w", err)
	}

	queue, err := c.ch.QueueDeclare(
		def.QueueName,
		def.DurableQueue,
		def.AutoDeleteQueue,
		// exclusive, we dont' support this since we use one connection for
		// consuming and one for producing.
		false,
		false, // no-wait
		def.QueueArgs,
	)
	if err != nil {
		return fmt.Errorf("could not declare queue: %w", err)
	}

	err = c.ch.QueueBind(
		queue.Name,
		def.RoutingKey,
		def.ExchangeName,
		false,
		def.BindHeaders,
	)

	if err != nil {
		return fmt.Errorf("could not bind queue: %w", err)
	}

	return err
}

func (c *ConsumerMultiplexer) startConsumer(wrapped wrappedConsumer) error {
	err := c.createTopology(wrapped.definintion)
	if err != nil {
		return err
	}

	fmt.Println("created topology")

	consumerMessages, err := c.ch.Consume(
		wrapped.definintion.QueueName,
		wrapped.definintion.Tag,
		wrapped.definintion.AutoAck,
		wrapped.definintion.Exclusive,
		false, // no-local is not supported by rabbitmq.
		false, // no-wait
		wrapped.definintion.Args,
	)
	if err != nil {
		return err
	}

	// Keep it so we can restart consumption on connection errors.
	c.consumers[wrapped.definintion.Tag] = wrapped

	c.wg.Add(1)

	go func() {
		fmt.Println("started inner consumer")
		for msg := range consumerMessages {
			fmt.Println("forwarding message")
			wrapped.messages <- msg
		}

		fmt.Println("finished inner consumer")

		c.wg.Done()
	}()

	return nil
}

func (c *ConsumerMultiplexer) Consume(def ConsumerDefinition) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	wrapped, ok := c.consumers[def.Tag]
	if ok {
		c.mu.Unlock()
		return wrapped.messages, nil
	}

	wrapped = wrappedConsumer{
		definintion: def,
		messages:    make(chan amqp.Delivery),
	}

	err := c.startConsumer(wrapped)

	c.mu.Unlock()

	return wrapped.messages, err
}
