package amqprpc

import (
	"log"
	"sync"
	"time"

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

	// middlewares holds slice of middlewares to run before or after the client
	// sends a request.
	middlewares []ClientMiddlewareFunc

	// stopChan channel is used to signal shutdowns when calling Stop(). The
	// channel will be closed when Stop() is called.
	stopChan chan struct{}

	// once is a sync Once which is used to only call ensure running once per
	// connection. When the runner for the publisher is stopped a new synx.Once
	// will be created to ensure the function connection and starting the
	// publisher will be called.
	once sync.Once

	// errorLog specifies an optional logger for amqp errors, unexpected behaviour etc.
	// If nil, logging is done via the log package's standard logger.
	errorLog LogFunc

	// debugLog specifies an optional logger for debugging, this logger will
	// print most of what is happening internally.
	// If nil, logging is not done.
	debugLog LogFunc
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
		once:               sync.Once{},
		errorLog:           log.Printf,                                  // use the standard logger default.
		debugLog:           func(format string, args ...interface{}) {}, // don't print anything default.
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

// WithErrorLogger sets the logger to use for error logging.
func (c *Client) WithErrorLogger(f LogFunc) *Client {
	c.errorLog = f
	return c
}

// WithDebugLogger sets the logger to use for debug logging.
func (c *Client) WithDebugLogger(f LogFunc) *Client {
	c.debugLog = f
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
// t will be rounded using the duration's Round function to the nearest
// multiple of a millisecond. Rounding will be away from zero.
func (c *Client) WithTimeout(t time.Duration) *Client {
	c.timeout = t.Round(time.Millisecond)
	return c
}

// AddMiddleware will add a middleware which will be executed on request.
func (c *Client) AddMiddleware(m ClientMiddlewareFunc) *Client {
	c.middlewares = append(c.middlewares, m)

	return c
}

func (c *Client) setDefaults() {
	c.queueDeclareSettings = QueueDeclareSettings{
		Durable:          true,
		DeleteWhenUnused: true,
		Exclusive:        false,
		NoWait:           false,
		Args:             nil,
	}

	c.consumeSettings = ConsumeSettings{
		Consumer:  "",
		AutoAck:   true,
		Exclusive: true,
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
		c.debugLog("client: connecting...")

		err := c.runOnce(url)
		if err == nil {
			c.debugLog("client: finished gracefully")
			break
		}

		c.errorLog("client: got error: %s, will reconnect in %v second(s)", err, 0.5)
		time.Sleep(500 * time.Millisecond)
	}
}

// runForever will connect amqp, setup all the amqp channels, run the publisher
// and run the replies consumer. The method will also return the underlying
// amqp error if the underlying connection or socket isn't gracefully closed.
// It will also block until the connection is gone.
func (c *Client) runOnce(url string) error {
	c.debugLog("client: starting up...")

	inputConn, outputConn, err := createConnections(c.url, c.dialconfig)
	if err != nil {
		return err
	}
	defer inputConn.Close()
	defer outputConn.Close()

	inputCh, outputCh, err := createChannels(inputConn, outputConn)
	if err != nil {
		return err
	}
	defer inputCh.Close()
	defer outputCh.Close()

	err = c.runRepliesConsumer(inputCh)
	if err != nil {
		return err
	}

	go c.runPublisher(outputCh)

	err = monitorAndWait(
		c.stopChan,
		inputConn.NotifyClose(make(chan *amqp.Error)),
		outputConn.NotifyClose(make(chan *amqp.Error)),
		inputCh.NotifyClose(make(chan *amqp.Error)),
		outputCh.NotifyClose(make(chan *amqp.Error)),
	)
	if err != nil {
		return err
	}

	// If the user calls Stop() but then starts to use the client again we must
	// ensure that a new connection will be setup. We do this by creating a new
	// sync.Once so the method runForever will be called again.
	c.once = sync.Once{}
	return nil
}

// runPublisher consumes messages from chan requests and publishes them on the
// amqp exchange. The method will stop consuming if the underlying amqp channel
// is closed for any reason, and when this happens the messages will be put back
// in chan requests unless we have retried to many times.
func (c *Client) runPublisher(outChan *amqp.Channel) {
	c.debugLog("client: running publisher...")

	for {
		select {
		case <-c.stopChan:
			c.debugLog("client: publisher stopped after stop chan was closed")
			return

		case request, _ := <-c.requests:
			replyToQueueName := ""

			if request.Reply {
				// We only need the replyTo queue if we actually want a reply.
				replyToQueueName = c.replyToQueueName
			}

			c.debugLog("client: publishing %s", request.Publishing.CorrelationId)

			request.Publishing.ReplyTo = replyToQueueName

			err := outChan.Publish(
				request.Exchange,
				request.RoutingKey,
				c.publishSettings.Mandatory,
				c.publishSettings.Immediate,
				request.Publishing,
			)

			if err != nil {
				// Close the outChan to ensure reconnect.
				outChan.Close()

				if request.numRetries > 0 {
					// The message that we tried to publish is NOT added back
					// to the queue since it never left the client. The sender
					// will get an error back and should handle this manually!
					c.errorLog(
						"client: could not publish %s, giving up: %s",
						request.Publishing.CorrelationId, err.Error(),
					)
					request.errChan <- err
				} else {
					c.errorLog(
						"client: could not publish %s, retrying: %s",
						request.Publishing.CorrelationId, err.Error(),
					)

					request.numRetries++
					c.requests <- request
				}

				c.errorLog("client: publisher stopped because of error, %s", request.Publishing.CorrelationId)
				return
			}

			if !request.Reply {
				// We don't expect a response, so just respond directly here
				// with nil to let send() return.
				request.response <- nil
			}

			c.debugLog("client: did publish %s", request.Publishing.CorrelationId)
		}
	}
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
		c.debugLog("client: running replies consumer...")

		for response := range messages {
			c.mu.RLock()
			replyChan, ok := c.correlationMapping[response.CorrelationId]
			c.mu.RUnlock()

			if !ok {
				c.errorLog("client: could not find where to reply. CorrelationId: %s", response.CorrelationId)
				continue
			}

			c.debugLog("client: forwarding reply %s", response.CorrelationId)

			replyChan <- &response
		}

		c.debugLog("client: replies consumer is done")
	}()

	return nil
}

// Send will send a Request by using a amqp.Publishing.
func (c *Client) Send(r *Request) (*amqp.Delivery, error) {
	// Ensure that the publisher is running. The Once is recreated on each
	// disconnect and since it has it's own mutex we don't need any other
	// locks.
	c.once.Do(
		func() {
			c.stopChan = make(chan struct{})

			go c.runForever(c.url)
		},
	)

	middlewares := append(c.middlewares, r.middlewares...)

	return ClientMiddlewareChain(c.send, middlewares...)(r)
}

func (c *Client) send(r *Request) (*amqp.Delivery, error) {
	// This is where we get the responses back.
	// If this request doesn't want a reply back (by setting Reply to false)
	// this channel will get a nil message after publisher has Published the
	// message.
	r.response = make(chan *amqp.Delivery)
	correlationID := uuid.Must(uuid.NewV4()).String()

	// Set the correlation id on the publishing.
	r.Publishing.CorrelationId = correlationID

	// This is where we get any (client) errors if they occure before we could
	// even send the request.
	r.errChan = make(chan error)

	// Ensure the responseConsumer will know which chan to forward the response
	// to when the response arrives.
	c.mu.Lock()
	c.correlationMapping[correlationID] = r.response
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.correlationMapping, correlationID)
		c.mu.Unlock()
	}()

	// If a request timeout is specified, use that one, otherwise use the
	// clients global timeout settings.
	if r.timeout.Nanoseconds() == 0 {
		r.timeout = c.timeout
	}

	// start the timeout counting now.
	timeoutChan := r.startTimeout()

	c.debugLog("client: queuing request %s", correlationID)
	c.requests <- r
	c.debugLog("client: waiting for reply of %s", correlationID)

	// All responses are published on the requests response channel. Hang here
	// until a response is received and close the channel when it's read.
	select {
	case err := <-r.errChan:
		c.debugLog("client: error for %s, %s", correlationID, err.Error())
		return nil, err
	case <-timeoutChan:
		c.debugLog("client: timeout for %s", correlationID)
		return nil, ErrTimeout
	case delivery := <-r.response:
		c.debugLog("client: got delivery for %s", correlationID)
		return delivery, nil
	}
}

// Stop will gracefully disconnect from AMQP after draining first incoming then
// outgoing messages. This method won't wait for server shutdown to complete,
// you should instead wait for ListenAndServe to exit.
func (c *Client) Stop() {
	close(c.stopChan)
}
