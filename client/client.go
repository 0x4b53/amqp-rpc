package client

import (
	"context"
	"errors"
	"time"

	"github.com/bombsimon/amqp-rpc/logger"
	"github.com/streadway/amqp"
)

var (
	// ErrTimeout is an error returned when a client request does not
	// receive a response within the client timeout duration.
	ErrTimeout = errors.New("request timed out")
)

type queueDeclareSettings struct {
	durable          bool
	deleteWhenUnused bool
	exclusive        bool
	noWait           bool
	args             amqp.Table
}

type consumeSettings struct {
	consumer  string
	autoAck   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	args      amqp.Table
}

// Client represents an AMQP client used within a RPC framework.
// This client can be used to communicate with RPC servers.
type Client struct {
	Exchange        string
	Timeout         time.Duration
	connection      *amqp.Connection
	consumerChannel *amqp.Channel
	context         context.Context
	dialconfig      amqp.Config
	queueDeclare    queueDeclareSettings
	consume         consumeSettings
}

func (c *Client) setDefaults() {
	c.queueDeclare = queueDeclareSettings{
		durable:         false,
		dleteWhenUnused: true,
		exclusive:       true,
		noWait:          false,
		args:            nil,
	}

	c.consume = consumeSettings{
		consumer:  "",
		autoAck:   true,
		exclusive: false,
		noLocal:   false,
		noWait:    false,
		args:      nil,
	}

	c.Timeout = 2000 * time.Millisecond

	ch, err := c.connection.Channel()
	if err != nil {
		logger.Warn("watcha gonna do...")
	}

	c.consumerChannel = ch
}

// New will return a pointer to a new Client.
func New(url string) *Client {
	c := &Client{
		context: context.Background(),
	}

	// Connect the client immediately
	c.connect(url)
	c.setDefaults()

	// Monitor the connection
	go func() {
		for {
			err, ok := <-c.connection.NotifyClose(make(chan *amqp.Error))
			if !ok {
				logger.Info("client connection closed")
				break
			}

			logger.Warnf("connection lost with an error: %s", err.Error())

			c.connect(url)
		}
	}()

	return c
}

// NewWithConnection return a pointer to a new Client with
// a connection created and monitored externally.
func NewWithConnection(conn *amqp.Connection) *Client {
	c := &Client{
		context:    context.Background(),
		connection: conn,
	}

	c.setDefaults()

	return c
}

func (c *Client) connect(url string) {
	for {
		conn, err := amqp.DialConfig(url, c.dialconfig)
		if err != nil {
			logger.Warnf("could not connect client: %s", err.Error())
			time.Sleep(1 * time.Second)
		}

		c.connection = conn
		break
	}
}

func (c *Client) Publish(routingKey string, body []byte, reply bool) (*amqp.Delivery, error) {
	messages, generatedQueue := c.declareAndConsume("")

	err := c.consumerChannel.Publish(
		c.Exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			ReplyTo:     generatedQueue,
			Body:        body,
		},
	)

	if err != nil {
		logger.Warn("could not publish message")
		return nil, err
	}

	if !reply {
		return nil, nil
	}

	select {
	case response, _ := <-messages:
		return &response, nil
	case <-time.After(c.Timeout):
		return nil, ErrTimeout
	}
}

func (c *Client) declareAndConsume(queueName string) (<-chan amqp.Delivery, string) {
	q, err := c.consumerChannel.QueueDeclare(
		queueName,
		c.queueDeclare.durable,
		c.queueDeclare.deleteWhenUnused,
		c.queueDeclare.exclusive,
		c.queueDeclare.noWait,
		c.queueDeclare.args,
	)

	if err != nil {
		logger.Warnf("could not declare queue")
	}

	messages, err := c.consumerChannel.Consume(
		q.Name,
		c.consume.consumer,
		c.consume.autoAck,
		c.consume.exclusive,
		c.consume.noLocal,
		c.consume.noWait,
		c.consume.args,
	)

	if err != nil {
		logger.Warnf("could not consume")
	}

	return messages, q.Name
}
