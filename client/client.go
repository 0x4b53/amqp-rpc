package client

import (
	"context"
	"errors"
	"time"

	"github.com/bombsimon/amqp-rpc/logger"
	uuid "github.com/satori/go.uuid"
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
	Exchange         string
	Timeout          time.Duration
	clientMessages   chan *clientPublish
	connection       *amqp.Connection
	consumerChannel  *amqp.Channel
	context          context.Context
	dialconfig       amqp.Config
	queueDeclare     queueDeclareSettings
	replyToQueueName string
	consume          consumeSettings
}

type clientPublish struct {
	routingKey string
	publishing *amqp.Publishing
	response   chan *amqp.Delivery
}

func (c *Client) setDefaults() {
	c.queueDeclare = queueDeclareSettings{
		durable:          false,
		deleteWhenUnused: true,
		exclusive:        true,
		noWait:           false,
		args:             nil,
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
		context:        context.Background(),
		clientMessages: make(chan *clientPublish),
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

	messages := c.declareAndConsume(c.replyToQueueName)

	var queueChannels = make(map[string]chan *amqp.Delivery)

	// Messages to publish
	go func() {
		for {
			logger.Info("Waint for messages to publish")
			request, _ := <-c.clientMessages

			logger.Infof("Got message, publishing on %s", request.routingKey)

			err := c.consumerChannel.Publish(
				c.Exchange,
				request.routingKey,
				false, // mandatory
				false, // immediate
				*request.publishing,
			)

			if err != nil {
				logger.Warn("could not publish message")
				// Add back to channel?
				continue
			}

			// Map a request correlation ID to a response channel
			if request.response != nil {
				logger.Info("Adding response to response loop channel")
				queueChannels[request.publishing.CorrelationId] = request.response
			}
		}
	}()

	// Responses from messages published
	go func() {
		for {
			logger.Info("Waiting for replys to respond")
			response, _ := <-messages
			logger.Infof("Got corrid '%s'", response.CorrelationId)

			if replyChannel, ok := queueChannels[response.CorrelationId]; ok {
				logger.Infof("Channel exist, adding response")
				replyChannel <- &response
			}
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

// SetConnection will set a new conection on the client.
// This should be used if the client was created with a connection
// outside this package and that connection is lost.
func (c *Client) SetConnection(conn *amqp.Connection) {
	c.connection = conn
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
	logger.Infof("Got request for %s", routingKey)

	var responseChannel chan *amqp.Delivery

	if reply {
		responseChannel = make(chan *amqp.Delivery)
	}

	request := &clientPublish{
		routingKey: routingKey,
		publishing: &amqp.Publishing{
			ContentType:   "text/plain",
			ReplyTo:       c.replyToQueueName,
			Body:          body,
			CorrelationId: uuid.Must(uuid.NewV4()).String(),
		},
		response: responseChannel,
	}

	logger.Infof("Putting on Go channel")
	c.clientMessages <- request

	if !reply {
		close(request.response)
		return nil, nil
	}

	logger.Info("Waiting for reply")
	delivery, ok := <-request.response
	if !ok {
		logger.Warnf("don't know yet...")
	}

	logger.Info("Got delivery, returning reply")

	close(request.response)

	return delivery, nil
}

func (c *Client) declareAndConsume(queueName string) <-chan amqp.Delivery {
	q, err := c.consumerChannel.QueueDeclare(
		c.replyToQueueName,
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

	c.replyToQueueName = q.Name

	return messages
}
