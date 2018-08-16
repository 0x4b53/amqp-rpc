package amqprpc

import (
	"sync"
	"time"

	"github.com/bombsimon/amqp-rpc/logger"
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

// Client represents an AMQP client used within a RPC framework.
// This client can be used to communicate with RPC servers.
type Client struct {
	// url is the URL where the server should dial to start subscribing.
	url string

	// timeout is the time we should wait after a request is sent before
	// we assume the request got lost.
	timeout time.Duration

	// requests is a single channel used whenever we want to publish
	// a message. The channel is consumed in a separate go routine which
	// allows us to add messages to the channel that we don't want replys from
	// without the need to wait for on going requests.
	requests chan *Request

	// correlationMapping maps each correlation ID to a channel of
	// *amqp.Delivery. This is to ensure that no matter the order of a request
	// and response, we will always publish the response to the correct
	// consumer. Since we create the correlation ID and hang waiting for the
	// delivery channel this is an easy way to ensure no overlapping. This can
	// of course be a problem if the correlation IDs used is not unique enough
	// in which case a queueChannel might be overridden if it hasn't been
	// cleared before.
	correlationMapping map[string]chan *amqp.Delivery

	// mu is used to protect the correlationMapping for concurrent access.
	mu sync.RWMutex

	// dialconfig is a amqp.Config which holds information about the connection
	// such as authentication, TLS configuration, and a dailer which is a
	// function used to obtain a connection.
	// By default the dialconfig will include a dail function implemented in
	// connection/dialer.go.
	dialconfig amqp.Config

	// queueDeclareSettings is configuration used when declaring a RabbitMQ
	// queue.
	queueDeclareSettings QueueDeclareSettings

	// consumeSettings is configuration used when consuming from the message
	// bus.
	consumeSettings ConsumeSettings

	// publishSettings is the configuration used when publishing a message with
	// the client
	publishSettings PublishSettings

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
	middlewares []ClientMiddlewareFunc

	// stopChan channel is used to signal shutdowns when calling Stop(). The
	// channel will be closed when Stop() is called.
	stopChan chan struct{}
}

// NewClient will return a pointer to a new Client. There are two ways to manage the
// connection that will be used by the client (i.e. when using TLS).
//
// The first one is to use the Certificates type and just pass the filenames to
// the client certificate, key and the server CA. If this is done the function
// will handle the reading of the files.
//
// It is also possible to create a custom amqp.Config with whatever
// configuration desired and that will be used as dial configuration when
// connection to the message bus.
func NewClient(url string) *Client {
	c := &Client{
		url: url,
		dialconfig: amqp.Config{
			Dial: DefaultDialer,
		},
		requests:           make(chan *Request),
		correlationMapping: make(map[string]chan *amqp.Delivery),
		mu:                 sync.RWMutex{},
		replyToQueueName:   "reply-to-" + uuid.Must(uuid.NewV4()).String(),
		middlewares:        []ClientMiddlewareFunc{},
		timeout:            time.Second * 10,
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
func (c *Client) WithTLS(cert Certificates) *Client {
	c.dialconfig.TLSClientConfig = cert.TLSConfig()

	return c
}

// WithQueueDeclareSettings will set the settings used when declaring queues
// for the client globally.
func (c *Client) WithQueueDeclareSettings(s QueueDeclareSettings) *Client {
	c.queueDeclareSettings = s

	return c
}

// WithConsumeSettings will set the settings used when consuming in the client
// globally.
func (c *Client) WithConsumeSettings(s ConsumeSettings) *Client {
	c.consumeSettings = s

	return c
}

// WithTimeout will set the client timeout used when publishing messages.
func (c *Client) WithTimeout(t time.Duration) *Client {
	c.timeout = t

	return c
}

// AddMiddleware will add a middleware which will be executed on request.
func (c *Client) AddMiddleware(m ClientMiddlewareFunc) *Client {
	c.middlewares = append(c.middlewares, m)

	return c
}

func (c *Client) setDefaults() {
	c.queueDeclareSettings = QueueDeclareSettings{
		Durable:          false,
		DeleteWhenUnused: true,
		Exclusive:        true,
		NoWait:           false,
		Args:             nil,
	}

	c.consumeSettings = ConsumeSettings{
		Consumer:  "",
		AutoAck:   true,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
		Args:      nil,
	}

	c.publishSettings = PublishSettings{
		Mandatory: false,
		Immediate: false,
	}
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

	err = c.runRepliesConsumer(inputCh)
	if err != nil {
		return err
	}

	c.runPublisher(outputCh)

	amqpChannel := conn.NotifyClose(make(chan *amqp.Error))
	err = monitorChannels(c.stopChan, []chan *amqp.Error{amqpChannel})

	// If the user calls Stop() but then starts to use the client again we must ensure that a new connection will be setup.
	c.publisherRunning = false

	// There's not really a need to ensure that te requests chan has published
	// all it's messages or that the responses for a given request is received
	// since the send function which will add messages to the queue are
	// blocking until the response has arrived. The only reason to check this
	// would be if a request was sent where a reply was not desired and the
	// user immediatle closed their program before the request was published. I
	// think this is something for the user of the client to handle.

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

// runPublisher consumes messages from chan requests and publishes them on the
// amqp exchange. The method will stop consuming if the underlying amqp channel
// is closed for any reason, and when this happens the messages will be put back
// in chan requests unless we have retried to many times.
func (c *Client) runPublisher(outChan *amqp.Channel) {
	logger.Info("client: running publisher")
	go func() {
		for request := range c.requests {
			replyToQueueName := ""

			if request.Reply {
				// We only need the replyTo queue if we actually want a reply.
				replyToQueueName = c.replyToQueueName
			}

			logger.Infof("client: publishing %v", request.correlationID)

			err := outChan.Publish(
				request.Exchange,
				request.RoutingKey,
				c.publishSettings.Mandatory,
				c.publishSettings.Immediate,
				amqp.Publishing{
					Headers:       request.Headers,
					ContentType:   request.ContentType,
					ReplyTo:       replyToQueueName,
					Body:          request.Body,
					CorrelationId: request.correlationID,
				},
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
					c.requests <- request
				}

				return
			}

			if !request.Reply {
				// We don't expect a response, so just respond directly here
				// with nil to let send() return.
				request.response <- nil
			}

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
			c.mu.RLock()
			replyChan, ok := c.correlationMapping[response.CorrelationId]
			c.mu.RUnlock()

			if !ok {
				logger.Warnf("client: could not find where to reply. CorrelationId: %v", response.CorrelationId)
				continue
			}

			logger.Infof("client: forwarding reply %v", response.CorrelationId)

			replyChan <- &response
		}

		logger.Info("client: done waiting for replies")
	}()

	return nil
}

func (c *Client) ensureRunning() {
	if !c.publisherRunning {
		c.stopChan = make(chan struct{})

		go c.runForever(c.url)

		c.publisherRunning = true
	}
}

// Send will send a Request by using a amqp.Publishing.
func (c *Client) Send(r *Request) (*amqp.Delivery, error) {
	c.ensureRunning()

	middlewares := append(c.middlewares, r.middlewares...)

	return ClientMiddlewareChain(c.send, middlewares...)(r)
}

func (c *Client) send(r *Request) (*amqp.Delivery, error) {
	// This is where we get the responses back.
	// If this request doesn't want a reply back (by setting Reply to false)
	// this channel will get a nil message after publisher has Published the
	// message.
	r.response = make(chan *amqp.Delivery)
	r.correlationID = uuid.Must(uuid.NewV4()).String()

	// This is where we get any (client) errors if they occure before we could
	// even send the request.
	r.errChan = make(chan error)

	// Ensure the responseConsumer will know which chan to forward the response
	// to when the response arrives.
	c.mu.Lock()
	c.correlationMapping[r.correlationID] = r.response
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.correlationMapping, r.correlationID)
		c.mu.Unlock()
	}()

	// If a request timeout is specified, use that one, otherwise use the
	// clients global timeout settings.
	var timeout time.Duration

	if r.Timeout.Nanoseconds() != 0 {
		timeout = r.Timeout
	} else if c.timeout.Nanoseconds() != 0 {
		timeout = c.timeout
	}

	c.requests <- r

	// All responses are published on the requests response channel. Hang here
	// until a response is received and close the channel when it's read.
	select {
	case err := <-r.errChan:
		return nil, err
	case <-time.After(timeout):
		return nil, ErrTimeout
	case delivery := <-r.response:
		logger.Info("client: got delivery")
		return delivery, nil
	}
}

// Stop will gracefully disconnect from AMQP after draining first incoming then
// outgoing messages. This method won't wait for server shutdown to complete,
// you should instead wait for ListenAndServe to exit.
func (c *Client) Stop() {
	close(c.stopChan)
}
