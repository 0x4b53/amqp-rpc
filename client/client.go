package client

import (
	"errors"
	"sync"
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
	// url is the URL where the server should dial to start subscribing.
	url string

	// Timeout is the time we should wait after a request is sent before
	// we assume the request got lost.
	Timeout time.Duration

	// clientMessages is a single channel used whenever we want to publish
	// a message. The channel is consumed in a separate go routine which
	// allows us to add messages to the channel that we don't want replys from
	// without the need to wait for on going requests.
	clientMessages chan *publishingRequestMessages

	// correlationMapping maps each correlation ID to a channel of
	// *amqp.Delivery. This is to ensure that no matter the order of a request
	// and response, we will always publish the response to the correct
	// consumer. Since we create the correlation ID and hang waiting for the
	// delivery channel this is an easy way to ensure no overlapping. This can
	// of course be a problem if the correlation IDs used is not unique enough
	// in which case a queueChannel might be overridden if it hasn't been
	// cleared before.
	correlationMapping map[string]chan amqp.Delivery

	// mu is used to protect the correlationMapping for concurrent access.
	mu sync.Mutex

	// dialconfig is a amqp.Config which holds information about the connection
	// such as authentication, TLS configuration, and a dailer which is a
	// function used to obtain a connection.
	// By default the dialconfig will include a dail function implemented in
	// connection/dialer.go.
	dialconfig amqp.Config

	// queueDeclareSettings is configuration used when declaring a RabbitMQ
	// queue.
	queueDeclareSettings connection.QueueDeclareSettings

	// consumeSettings is configuration used when consuming from the message
	// bus.
	consumeSettings connection.ConsumeSettings

	// publishSettings is the configuration used when publishing a message with
	// the client
	publishSettings connection.PublishSettings

	// replyToQueueName can be used to avoid generating queue names on the message
	// bus and use a pre defined name throughout the usage of a client.
	replyToQueueName string

	// running holds the state telling if the client is running the publisher
	// and reply consumer. By default this is false when a client is created
	// but will be set to true after the runner is called the first time the
	// client needs to connect.
	publisherRunning bool

	// middlewares holds slice of middlewares to run before or after the client
	// sends a request.
	middlewares []MiddlewareFunc
}

// publishingRequestMessages is a type that holds information about each request
// that should be published. Besides the amqp.Publishing and routing key, the type
// has one channel for responses after the message is sent and one for errors
// if an error occurrs when trying to send the message.
type publishingRequestMessages struct {
	exchange   string
	routingKey string
	publishing amqp.Publishing
	response   chan amqp.Delivery
	errChan    chan error
	numRetries int
}

// New will return a pointer to a new Client. There are two ways to manage the
// connection that will be used by the client (i.e. when using TLS).
//
// The first one is to use the Certificates type and just pass the filenames to
// the client certificate, key and the server CA. If this is done the function
// will handle the reading of the files.
//
// It is also possible to create a custom amqp.Config with whatever
// configuration desired and that will be used as dial configuration when
// connection to the message bus.
func New(url string) *Client {
	c := &Client{
		url: url,
		dialconfig: amqp.Config{
			Dial: connection.DefaultDialer,
		},
		clientMessages:     make(chan *publishingRequestMessages),
		correlationMapping: make(map[string]chan amqp.Delivery),
		mu:                 sync.Mutex{},
		replyToQueueName:   "reply-to-" + uuid.Must(uuid.NewV4()).String(),
		middlewares:        []MiddlewareFunc{},
	}

	// Set default values to use when crearing channels and consumers.
	c.setDefaults()

	return c
}

// WithDialConfig sets the dial config used for the client.
func (c *Client) WithDialConfig(dc amqp.Config) *Client {
	c.dialconfig = dc

	return c
}

// WithTLS sets the TLS config in the dial config for the client.
func (c *Client) WithTLS(cert connection.Certificates) *Client {
	c.dialconfig.TLSClientConfig = cert.TLSConfig()

	return c
}

// WithQueueDeclareSettings will set the settings used when declaring queues
// for the client globally.
func (c *Client) WithQueueDeclareSettings(s connection.QueueDeclareSettings) *Client {
	c.queueDeclareSettings = s

	return c
}

// WithConsumeSettings will set the settings used when consuming in the client
// globally.
func (c *Client) WithConsumeSettings(s connection.ConsumeSettings) *Client {
	c.consumeSettings = s

	return c
}

// WithTimeout will set the client timeout used when publishing messages.
func (c *Client) WithTimeout(t time.Duration) *Client {
	c.Timeout = t

	return c
}

// AddMiddleware will add a middleware which will be executed on request.
func (c *Client) AddMiddleware(m MiddlewareFunc) *Client {
	c.middlewares = append(c.middlewares, m)

	return c
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

// runForever will connect amqp, setup all the amqp channels, run the publisher
// and run the replies consumer. The method will also automatically restart
// the setup if the underlying connection or socket isn't gracefully closed.
// This will also block until the client is gracefully stopped.
func (c *Client) runForever(url string) {
	for {
		logger.Info("client: connecting...")
		err := c.runOnce(url)

		if err != nil {
			logger.Warnf("client: got error: %s, will reconnect in %v second(s)", err, 0.5)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		logger.Info("client: finished gracefully")
		break
	}
}

// runForever will connect amqp, setup all the amqp channels, run the publisher
// and run the replies consumer. The method will also return the underlying
// amqp error if the underlying connection or socket isn't gracefully closed.
// It will also block until the connection is gone.
func (c *Client) runOnce(url string) error {
	logger.Info("client: starting up")

	conn, err := amqp.DialConfig(url, c.dialconfig)
	if err != nil {
		return err
	}

	defer conn.Close()

	inputCh, outputCh, err := c.declareChannels(conn)
	if err != nil {
		return err
	}

	defer inputCh.Close()
	defer outputCh.Close()

	c.runRepliesConsumer(inputCh)
	c.runPublisher(outputCh)

	err, ok := <-conn.NotifyClose(make(chan *amqp.Error))
	if !ok {
		// The connection was closed gracefully.
		return nil
	}
	// The connection wasn't closed gracefully.
	return err
}

// Declare input- and output channels to use when publishing and consuming data.
func (c *Client) declareChannels(conn *amqp.Connection) (*amqp.Channel, *amqp.Channel, error) {
	inputCh, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	outputCh, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return inputCh, outputCh, nil
}

// runPublisher consumes messages from clientMessages and publishes them on the
// amqp exchange. The method will stop consuming if the underlying amqp channel
// is closed for any reason, and when this happens the messages will be put back
// in clientMessages unless we have retried to many times.
func (c *Client) runPublisher(outChan *amqp.Channel) {
	logger.Info("client: running publisher")
	go func() {
		for request := range c.clientMessages {
			logger.Infof("client: publishing %v", request.publishing.CorrelationId)

			err := outChan.Publish(
				request.exchange,
				request.routingKey,
				c.publishSettings.Mandatory,
				c.publishSettings.Immediate,
				request.publishing,
			)

			if err != nil {
				if request.numRetries >= 0 {
					// The message that we tried to publish is NOT added back
					// to the queue since it never left the client. The sender
					// will get an error back and should handle this manually!
					logger.Warn("client: could not publish message, giving up")
					request.errChan <- err
				} else {
					logger.Warn("client: could not publish message, retrying")
					request.numRetries++
					c.clientMessages <- request
				}

				return
			}

			// Map a request correlation ID to a response channel. The mutext
			// lock taken in the response consumer should be sufficient to
			// handle potential races. Famous last words. If race problems are
			// detected, make sure to take this lock *before* publishing on the
			// out channel.
			//
			// This is not needed if we don't want a reply, that's just a risk
			// for us to hang in the consumer if not handled properly.
			if request.response != nil {
				c.mu.Lock()
				c.correlationMapping[request.publishing.CorrelationId] = request.response
				c.mu.Unlock()
			}

			// The request has left the client, no more errors can occur from
			// here.
			close(request.errChan)

			logger.Info("client: did publish message")
		}
	}()
}

// runRepliesConsumer will declare and start consuming from the queue where we
// expect replies to come back. The method will stop consuming if the
// underlying amqp channel is closed for any reason.
func (c *Client) runRepliesConsumer(inChan *amqp.Channel) error {
	queue, err := inChan.QueueDeclare(
		c.replyToQueueName,
		c.queueDeclareSettings.Durable,
		c.queueDeclareSettings.DeleteWhenUnused,
		c.queueDeclareSettings.Exclusive,
		c.queueDeclareSettings.NoWait,
		c.queueDeclareSettings.Args,
	)

	if err != nil {
		return err
	}

	messages, err := inChan.Consume(
		queue.Name,
		c.consumeSettings.Consumer,
		c.consumeSettings.AutoAck,
		c.consumeSettings.Exclusive,
		c.consumeSettings.NoLocal,
		c.consumeSettings.NoWait,
		c.consumeSettings.Args,
	)

	if err != nil {
		return err
	}

	go func() {
		logger.Info("client: waiting for replies")
		for response := range messages {
			c.mu.Lock()
			replyChan, ok := c.correlationMapping[response.CorrelationId]
			if !ok {
				logger.Warnf("client: could not find where to reply. CorrelationId: %v", response.CorrelationId)
				c.mu.Unlock()
				continue
			}
			// Remove the mapping between correlation ID and reply channel. We
			// don't need it any more.
			delete(c.correlationMapping, response.CorrelationId)
			c.mu.Unlock()

			logger.Infof("client: forwarding reply %v", response.CorrelationId)

			replyChan <- response
			close(replyChan)
		}

		logger.Info("client: done waiting for replies")
	}()

	return nil
}

func (c *Client) ensureRunning() {
	if !c.publisherRunning {
		go c.runForever(c.url)

		c.publisherRunning = true
	}
}

// Send will send a Request by using a amqp.Publishing.
func (c *Client) Send(r *Request) (*amqp.Delivery, error) {
	c.ensureRunning()

	middlewares := append(c.middlewares, r.middlewares...)

	return MiddlewareChain(c.send, middlewares...)(r)
}

func (c *Client) send(r *Request) (*amqp.Delivery, error) {
	// Init a channel to receive responses. A channel is used to be
	// non-blocking.
	var responseChannel chan amqp.Delivery

	// Setup default content type
	var contentType = "text/plain"

	if ct, ok := r.Headers["ContentType"]; ok {
		contentType = ct.(string)
	}

	// Only define the channel if we're going to used.
	if r.Reply {
		responseChannel = make(chan amqp.Delivery)
	}

	logger.Infof("client: sender: replyChan is %v", responseChannel)
	request := &publishingRequestMessages{
		exchange:   r.Exchange,
		routingKey: r.RoutingKey,
		publishing: amqp.Publishing{
			Headers:       r.Headers,
			ContentType:   contentType,
			ReplyTo:       c.replyToQueueName,
			Body:          r.Body,
			CorrelationId: uuid.Must(uuid.NewV4()).String(),
		},
		response: responseChannel,
		errChan:  make(chan error),
	}

	c.clientMessages <- request

	logger.Info("client: waiting for error")
	err, _ := <-request.errChan
	// Ignore closed channels, a channel is closed if no error occurs.

	if err != nil {
		return nil, err
	}

	logger.Info("client: no error")

	// Don't wait for reply if the requests wishes to ignore them.
	if !r.Reply {
		return nil, nil
	}

	// All responses are published on the requests response channel. Hang here
	// until a response is received and close the channel when it's read.
	logger.Info("client: waiting for delivery")
	delivery, ok := <-request.response
	if !ok {
		logger.Warnf("client: response channel was closed")
		return nil, errors.New("client: no response")
	}

	logger.Info("client: got delivery")

	return &delivery, nil
}
