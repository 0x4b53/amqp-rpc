package client

import (
	"context"
	"errors"
	"time"

	rpcconn "github.com/bombsimon/amqp-rpc/connection"
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
		logger.Warn("could not create consumer channel")
	}

	c.consumerChannel = ch
}

// New will return a pointer to a new Client.
// There are two ways to manage the connection that will be used
// by the client (i.e. when using TLS).
//
// The first one is to use the Certificates type and just pass the
// filenames to the client certificate, key and the server CA. If
// this is done the function will handle the reading of the files.
//
// It is also possible to create a custom amqp.Config with whatever
// configuration desired and that will be used as dial configuration
// when connection to the message bus.
func New(url string, args ...interface{}) *Client {
	c := &Client{
		context:        context.TODO(),
		clientMessages: make(chan *clientPublish),
	}

	// Scan arguments for amqp.Config or Certificates config
	for _, arg := range args {
		switch v := arg.(type) {
		case amqp.Config:
			c.dialconfig = v

		case rpcconn.Certificates:
			// Set the TLSClientConfig in the dialconfig
			c.dialconfig = amqp.Config{
				TLSClientConfig: v.TLSConfig(),
			}
		}
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

	c.setupChannels()

	return c
}

// NewWithConnection return a pointer to a new Client with
// a connection created and monitored externally.
func NewWithConnection(conn *amqp.Connection) *Client {
	c := &Client{
		context:        context.TODO(),
		connection:     conn,
		clientMessages: make(chan *clientPublish),
	}

	c.setDefaults()
	c.setupChannels()

	return c
}

func (c *Client) setupChannels() {
	messages := c.declareAndConsume()

	var queueChannels = make(map[string]chan *amqp.Delivery)

	// Messages to publish
	go func() {
		for {
			request, ok := <-c.clientMessages
			if !ok {
				logger.Info("client message channel was closed")
				break
			}

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
				queueChannels[request.publishing.CorrelationId] = request.response
			}
		}
	}()

	// Responses from messages published
	go func() {
		for {
			response, _ := <-messages

			// Check if the published message correlation ID is maped.
			// This should be true as long as the caller of publish requested a reply.
			if replyChannel, ok := queueChannels[response.CorrelationId]; ok {
				replyChannel <- &response

				// Remove the mapping between correlation ID and reply channel.
				delete(queueChannels, response.CorrelationId)
			}
		}
	}()
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

// Publish takes a string with a routing key, a byte slice with a body and a bool
// set to true or false depending on if a response is desired.
// Each request will create a channel on which the response will be added when
// received. If no response is requested, no channel will be created.
func (c *Client) Publish(routingKey string, body []byte, reply bool) (*amqp.Delivery, error) {
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

	c.clientMessages <- request

	if !reply {
		return nil, nil
	}

	delivery, ok := <-request.response
	if !ok {
		logger.Warnf("response channel was closed")
	}

	close(request.response)

	return delivery, nil
}

func (c *Client) declareAndConsume() <-chan amqp.Delivery {
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
