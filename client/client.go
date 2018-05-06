package client

import (
	"errors"
	"time"

	"github.com/bombsimon/amqp-rpc/connection"
	"github.com/bombsimon/amqp-rpc/logger"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

var (
	// ErrTimeout is an error returned when a client request does not
	// receive a response within the client timeout duration.
	ErrTimeout = errors.New("request timed out")
)

// Client represents an AMQP client used within a RPC framework.
// This client can be used to communicate with RPC servers.
type Client struct {
	// Exchange is the RabbitMQ exchange to publish all messages on.
	// This exchange will default to an empty string which is also called
	// amq.default.
	Exchange string

	// Timeout is the time we should wait after a request is sent before
	// we assume the request got lost.
	Timeout time.Duration

	// clientMessages is a single channel used whenever we want to publish
	// a message. The channel is consumed in a separate go routine which
	// allows us to add messages to the channel that we don't want replys from
	// without the need to wait for on going requests.
	clientMessages chan *publishingRequestMessages

	// connection is the underlying amqp.Connection we get from amqp.DialConfig.
	// This connection is used whenever we want to interact with the messagebus
	// like when we create a consumer channel or monitoring the connection with
	// NotifyClose.
	connection *amqp.Connection

	// consumerChannel is created after we connect (or get passed a connection).
	// This is the channel we use to declare and consume from a AMQP queue if we
	// wish to receive responses for our requests. This cahnnel is also used
	// when publishing messages to the message bus.
	consumerChannel *amqp.Channel

	// dialconfig is a amqp.Config which holds information about the connection
	// such as authentication, TLS configuration, and a dailer which is a
	// function used to obtain a connection.
	// By default the dialconfig will include a dail function implemented in
	// connection/dialer.go.
	dialconfig amqp.Config

	// queueDeclareSettings is configuration used when declaring a RabbitMQ queue.
	queueDeclareSettings connection.QueueDeclareSettings

	// consumeSettings is configuration used when consuming from the message bus.
	consumeSettings connection.ConsumeSettings

	// publishSettings is the configuration used when publishing a message with the client
	publishSettings connection.PublishSettings

	// replyToQueueName can be used to avoid generating queue names on the message
	// bus and use a pre defined name throughout the usage of a client.
	replyToQueueName string

	// Connected is the state of the client which will be true if the client has
	// an active connection to the message bus. When a client is disconnected
	// it will try to reconnect until connected.
	connected bool
}

// publishingRequestMessages is a type that holds information about each request
// that should be published. Besides the amqp.Publishing and routing key, the type
// has one channel for responses after the message is sent and one for errors
// if an error occurrs when trying to send the message.
type publishingRequestMessages struct {
	routingKey string
	publishing *amqp.Publishing
	response   chan *amqp.Delivery
	errChan    chan error
}

func (c *Client) setDefaults() {
	c.queueDeclareSettings = connection.QueueDeclareSettings{
		Durable:          false,
		DeleteWhenUnused: true,
		Exclusive:        true,
		NoWait:           false,
		Args:             nil,
	}

	c.consumeSettings = connection.ConsumeSettings{
		Consumer:  "",
		AutoAck:   true,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
		Args:      nil,
	}

	c.publishSettings = connection.PublishSettings{
		Mandatory: false,
		Immediate: false,
	}

	c.Timeout = 2000 * time.Millisecond
}

// createConsumerChannel will use the client connection and create
// a amqp channel which is later used to send and receive messages.
func (c *Client) createConsumerChannel() {
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
		dialconfig: amqp.Config{
			Dial: connection.DefaultDialer,
		},
		clientMessages: make(chan *publishingRequestMessages),
	}

	// Scan arguments for amqp.Config or Certificates config
	for _, arg := range args {
		switch v := arg.(type) {
		case amqp.Config:
			c.dialconfig = v

		case connection.Certificates:
			// Set the TLSClientConfig in the dialconfig
			c.dialconfig.TLSClientConfig = v.TLSConfig()
		}
	}

	// Connect the client immediately.
	c.connect(url)

	// Watch for connection notificates in a separate go routinge.
	go c.monitorConnection(url)

	// Set default values to use when crearing channels
	// and consumers.
	c.setDefaults()

	// Create the consumer channel from the connection
	// set when connecting to the message bus.
	c.createConsumerChannel()

	c.setupChannels()

	return c
}

func (c *Client) monitorConnection(url string) {
	for {
		err, ok := <-c.connection.NotifyClose(make(chan *amqp.Error))
		if !ok {
			logger.Info("client connection closed")
			break
		}

		logger.Warnf("connection lost with an error: %s", err.Error())

		c.connect(url)
		c.createConsumerChannel()
	}
}

func (c *Client) setupChannels() {
	messages := c.declareAndConsume()

	// queueChannels maps each correlation ID to a channel of *amqp.Delivery.
	// This is to ensure that no matter the order of a request and response,
	// we will always publish the response to the correct consumer.
	// Since we create the correlation ID and hangs waiting for the delivery
	// channel this is an easy way to ensure no overlapping.
	// This can of course be a problem if the correlation IDs used is not unique
	// enough in which case a queueChannel might be overridden if it hasn't
	// been cleared before.
	var queueChannels = make(map[string]chan *amqp.Delivery)

	// We spawn a go routine which will listen on the clients message channel.
	// Each message we receive will be published. If the sender wishes to
	// receive a response we also map the response channel to the correlation ID.
	// If we meet an early error before even publishing we send the error back
	// on that queue.
	go func() {
		for {
			request, ok := <-c.clientMessages
			if !ok {
				logger.Info("client message channel was closed")
				break
			}

			// Map a request correlation ID to a response channel
			if request.response != nil {
				queueChannels[request.publishing.CorrelationId] = request.response
			}

			err := c.consumerChannel.Publish(
				c.Exchange,
				request.routingKey,
				c.publishSettings.Mandatory,
				c.publishSettings.Immediate,
				*request.publishing,
			)

			if err != nil {
				logger.Warn("could not publish message")
				request.errChan <- err

				// The message that we tried to publish is NOT added back to the
				// queue since it never left the client. The sender will get
				// an error back and should handle this manually!
				continue
			}

			// If no error is met, close the error channel so the sender doesn't
			// hang forever.
			close(request.errChan)
		}
	}()

	// We consume messages from the same queue as we publish them but we watch for
	// responses in a separate go routine. All requests sent will get a response
	// back on the consumer channel but we only pass them to the user of the
	// client if we mapped the correlation ID to a response channel.
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

// Send will send a Request by using a amqp.Publishing.
func (c *Client) Send(r *Request) (*amqp.Delivery, error) {
	// Init a channel to receive responses.
	// A channel is used to be non-blocking.
	var responseChannel chan *amqp.Delivery

	// Only define the channel if we're going to used.
	if r.Reply {
		responseChannel = make(chan *amqp.Delivery)
	}

	request := &publishingRequestMessages{
		routingKey: r.RoutingKey,
		publishing: &amqp.Publishing{
			ContentType:   r.Header["ContentType"].(string),
			ReplyTo:       c.replyToQueueName,
			Body:          r.Body,
			CorrelationId: uuid.Must(uuid.NewV4()).String(),
		},
		response: responseChannel,
		errChan:  make(chan error),
	}

	c.clientMessages <- request

	err, _ := <-request.errChan
	// Ignore closed channels, a channel is closed if no error occurs.

	if err != nil {
		return nil, err
	}

	// Don't wait for reply if the requests wishes to ignore them.
	if !r.Reply {
		return nil, nil
	}

	// All responses are published on the requests response channel.
	// Hang here until a response is received and close the channel
	// when it's read.
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
		c.queueDeclareSettings.Durable,
		c.queueDeclareSettings.DeleteWhenUnused,
		c.queueDeclareSettings.Exclusive,
		c.queueDeclareSettings.NoWait,
		c.queueDeclareSettings.Args,
	)

	if err != nil {
		logger.Warnf("could not declare queue")
	}

	messages, err := c.consumerChannel.Consume(
		q.Name,
		c.consumeSettings.Consumer,
		c.consumeSettings.AutoAck,
		c.consumeSettings.Exclusive,
		c.consumeSettings.NoLocal,
		c.consumeSettings.NoWait,
		c.consumeSettings.Args,
	)

	if err != nil {
		logger.Warnf("could not consume")
	}

	c.replyToQueueName = q.Name

	return messages
}
