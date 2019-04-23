package amqprpc

import (
	"crypto/tls"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
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

	// isRunning is 1 when the server is running.
	isRunning int32

	// errorLog specifies an optional logger for amqp errors, unexpected
	// behavior etc. If nil, logging is done via the log package's standard
	// logger.
	errorLog LogFunc

	// debugLog specifies an optional logger for debugging, this logger will
	// print most of what is happening internally.
	// If nil, logging is not done.
	debugLog LogFunc

	// Sender is the main send function called after all middlewares has been
	// chained and called. This field can be overridden to simplify testing.
	Sender SendFunc

	// onStarteds will all be executed after the client has connected.
	onStarteds []OnStartedFunc
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
		replyToQueueName:   "reply-to-" + uuid.New().String(),
		middlewares:        []ClientMiddlewareFunc{},
		timeout:            time.Second * 10,
		errorLog:           log.Printf,                                  // use the standard logger default.
		debugLog:           func(format string, args ...interface{}) {}, // don't print anything default.
	}

	c.Sender = c.send

	// Set default values to use when crearing channels and consumers.
	c.setDefaults()

	return c
}

/*
OnStarted can be used to hook into the connections/channels that the client is
using. This can be useful if you want more control over amqp directly.
Note that since the client is lazy and won't connect until the first .Send()
the provided OnStartedFunc won't be called until then. Also note that this
is blocking and the client won't continue it's startup until this function has
finished executing.

	client := NewClient(url)
	client.OnStarted(func(inConn, outConn *amqp.Connection, inChan, outChan *amqp.Channel) {
		// Do something with amqp connections/channels.
	})
*/
func (c *Client) OnStarted(f OnStartedFunc) {
	c.onStarteds = append(c.onStarteds, f)
}

// WithDialConfig sets the dial config used for the client.
func (c *Client) WithDialConfig(dc amqp.Config) *Client {
	c.dialconfig = dc

	return c
}

// WithTLS sets the TLS config in the dial config for the client.
func (c *Client) WithTLS(tls *tls.Config) *Client {
	c.dialconfig.TLSClientConfig = tls

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

// WithPublishSettings will set the client publishing settings when publishing
// messages.
func (c *Client) WithPublishSettings(s PublishSettings) *Client {
	c.publishSettings = s

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
func (c *Client) runForever() {
	if !atomic.CompareAndSwapInt32(&c.isRunning, 0, 1) {
		// Already running.
		return
	}

	c.stopChan = make(chan struct{})

	go func() {
		for {
			c.debugLog("client: connecting...")

			err := c.runOnce()
			if err == nil {
				c.debugLog("client: finished gracefully")
				break
			}

			c.errorLog("client: got error: %s, will reconnect in %v second(s)", err, 0.5)
			time.Sleep(500 * time.Millisecond)
		}

		// Ensure we can start again.
		atomic.StoreInt32(&c.isRunning, 0)
	}()

}

// runOnce will connect amqp, setup all the amqp channels, run the publisher
// and run the replies consumer. The method will also return the underlying
// amqp error if the underlying connection or socket isn't gracefully closed.
// It will also block until the connection is gone.
func (c *Client) runOnce() error {
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

	// Notify everyone that the client has started. Runs sequentially so there
	// isn't any race conditions when working with the connections or channels.
	for _, onStarted := range c.onStarteds {
		onStarted(inputConn, outputConn, inputCh, outputCh)
	}

	err = c.runRepliesConsumer(inputCh)
	if err != nil {
		return err
	}

	go c.runPublisher(outputCh, c.stopChan)

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

	return nil
}

// runPublisher consumes messages from chan requests and publishes them on the
// amqp exchange. The method will stop consuming if the underlying amqp channel
// is closed for any reason, and when this happens the messages will be put back
// in chan requests unless we have retried to many times.
func (c *Client) runPublisher(outChan *amqp.Channel, stopChan chan struct{}) {
	c.debugLog("client: running publisher...")

	for {
		select {
		case <-stopChan:
			c.debugLog("client: publisher stopped after stop chan was closed")
			return

		case request := <-c.requests:
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

			responseCopy := response
			replyChan <- &responseCopy
		}

		c.debugLog("client: replies consumer is done")
	}()

	return nil
}

// Send will send a Request by using a amqp.Publishing.
func (c *Client) Send(r *Request) (*amqp.Delivery, error) {
	middlewares := append(c.middlewares, r.middlewares...)

	return ClientMiddlewareChain(c.Sender, middlewares...)(r)
}

func (c *Client) send(r *Request) (*amqp.Delivery, error) {
	// Ensure that the publisher is running.
	c.runForever()

	// This is where we get the responses back.
	// If this request doesn't want a reply back (by setting Reply to false)
	// this channel will get a nil message after publisher has Published the
	// message.
	r.response = make(chan *amqp.Delivery)

	// Set the correlation id on the publishing if not yet set.
	if r.Publishing.CorrelationId == "" {
		r.Publishing.CorrelationId = uuid.New().String()
	}

	// This is where we get any (client) errors if they occure before we could
	// even send the request.
	r.errChan = make(chan error)

	// Ensure the responseConsumer will know which chan to forward the response
	// to when the response arrives.
	c.mu.Lock()
	c.correlationMapping[r.Publishing.CorrelationId] = r.response
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.correlationMapping, r.Publishing.CorrelationId)
		c.mu.Unlock()
	}()

	// If a request timeout is specified, use that one, otherwise use the
	// clients global timeout settings.
	if r.Timeout.Nanoseconds() == 0 {
		r.Timeout = c.timeout
	}

	timeoutChan := r.startTimeout()

	c.debugLog("client: queuing request %s", r.Publishing.CorrelationId)
	c.requests <- r
	c.debugLog("client: waiting for reply of %s", r.Publishing.CorrelationId)

	// All responses are published on the requests response channel. Hang here
	// until a response is received and close the channel when it's read.
	select {
	case err := <-r.errChan:
		c.debugLog("client: error for %s, %s", r.Publishing.CorrelationId, err.Error())
		return nil, err
	case <-timeoutChan:
		c.debugLog("client: timeout for %s", r.Publishing.CorrelationId)
		return nil, ErrTimeout
	case delivery := <-r.response:
		c.debugLog("client: got delivery for %s", r.Publishing.CorrelationId)
		return delivery, nil
	}
}

// Stop will gracefully disconnect from AMQP after draining first incoming then
// outgoing messages. This method won't wait for server shutdown to complete,
// you should instead wait for ListenAndServe to exit.
func (c *Client) Stop() {
	if atomic.LoadInt32(&c.isRunning) != 1 {
		return
	}

	close(c.stopChan)
}
