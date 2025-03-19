package amqprpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// ErrRequestReturned can be returned by Client#Send() when the server
	// returns the message. For example when mandatory is set but the message
	// can not be routed.
	ErrRequestReturned = errors.New("publishing returned")

	// ErrRequestRejected can be returned by Client#Send() when the server Nacks
	// the message. This can happen if there is some problem inside the amqp
	// server. To check if the error returned is a ErrRequestReturned error, use
	// errors.Is(err, ErrRequestRejected).
	ErrRequestRejected = errors.New("publishing Nacked")

	// ErrRequestTimeout is an error returned when a client request does not
	// receive a response within the client timeout duration. To check if the
	// error returned is an ErrRequestTimeout error, use errors.Is(err,
	// ErrRequestTimeout).
	ErrRequestTimeout = errors.New("request timed out")
)

// Client represents an AMQP client used within a RPC framework.
// This client can be used to communicate with RPC servers.
type Client struct {
	// url is the URL where the server should dial to start subscribing.
	url string

	// name is the name of the client, it will be used the creation of the
	// reply-to queue, consumer tags and connection names.
	name string

	// timeout is the time we should wait after a request is published before
	// we assume the request got lost.
	timeout time.Duration

	// maxRetries is the amount of times a request will be retried before
	// giving up.
	maxRetries int

	// requests is a single channel used whenever we want to publish a message.
	// The channel is consumed in a separate go routine which allows us to add
	// messages to the channel that we don't want replies from without the need
	// to wait for on going requests.
	requests chan *Request

	// requestsMap will keep track of requests waiting for confirmations,
	// replies or returns. it maps each correlation ID and delivery tag of a
	// message to the actual Request. This is to ensure that no matter the order
	// of a request and response, we will always publish the response to the
	// correct consumer.
	requestsMap RequestMap

	// dialconfig is a amqp.Config which holds information about the connection
	// such as authentication, TLS configuration, and a dialer which is a
	// function used to obtain a connection.
	// By default the dialconfig will include a dial function implemented in
	// connection/dialer.go.
	dialconfig amqp.Config

	// replyToQueueDeclareArgs are any extra args that should be used when
	// declaring the reply-to queue.
	replyToQueueDeclareArgs amqp.Table

	// replyToConsumerArgs are any extra args that should be used when
	// consuming from the reply-to queue.
	replyToConsumerArgs amqp.Table

	// replyToQueueName can be used to avoid generating queue names on the
	// message bus and use a pre defined name throughout the usage of a client.
	replyToQueueName string

	// confirmMode enables confirmation mode when publishing messages.
	confirmMode bool

	// middlewares holds slice of middlewares to run before or after the client
	// sends a request.
	middlewares []ClientMiddlewareFunc

	// stopChan channel is used to signal shutdowns when calling Stop(). The
	// channel will be closed when Stop() is called.
	stopChan chan struct{}

	// didStopChan will close when the client has finished shutdown.
	didStopChan chan struct{}

	// isRunning is 1 when the server is running.
	isRunning int32

	// wantStop tells the runForever function to exit even on connection errors.
	wantStop int32

	logger *slog.Logger

	// Sender is the main send function called after all middlewares has been
	// chained and called. This field can be overridden to simplify testing.
	Sender SendFunc

	// onConnectedFuncs will all be executed after the client has connected.
	onConnectedFuncs []OnConnectedFunc

	// onErrorFuncs will all be executed when the client gets errors outside
	// the normal request-response cycle.
	onErrorFuncs []OnErrorFunc
}

// NewClient will return a pointer to a new Client. There are two ways to manage
// the connection that will be used by the client (i.e. when using TLS).
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
			Dial: amqp.DefaultDial(2 * time.Second),
		},
		requests: make(chan *Request),
		requestsMap: RequestMap{
			byDeliveryTag:   make(map[uint64]*Request),
			byCorrelationID: make(map[string]*Request),
		},
		name:        "amqprpc-client",
		middlewares: []ClientMiddlewareFunc{},
		timeout:     time.Second * 10,
		maxRetries:  10,
	}

	// use the standard logger default.
	c.WithLogger(slog.Default())

	c.Sender = c.send

	// Set default values to use when creating channels and consumers.
	c.setDefaults()

	return c
}

// OnConnected can be used to hook into the connections/channels that the client
// is using. This can be useful if you want more control over amqp directly. Note
// that this is blocking and the client won't continue it's startup until this
// function has finished executing.
//
//	client := NewClient(url)
//	client.OnConnected(func(inConn, outConn *amqp.Connection, inChan, outChan *amqp.Channel) {
//		// Do something with amqp connections/channels.
//	})
func (c *Client) OnConnected(f OnConnectedFunc) {
	c.onConnectedFuncs = append(c.onConnectedFuncs, f)
}

// OnError can be used to hook into the errors that the client encounters
// outside the normal request/response cycle. Note that it is not possible to
// call [Client.Stop] inside f. If you want to stop the client from here, you
// should use a goroutine. This is because [Client.Stop] will block until the
// client has stopped.
func (c *Client) OnError(f OnErrorFunc) {
	c.onErrorFuncs = append(c.onErrorFuncs, f)
}

// WithName sets the name of the client, it will be used the creation of the
// reply-to queue, consumer tags and connection names.
func (c *Client) WithName(name string) *Client {
	c.name = name

	return c
}

// WithDialConfig sets the dial config used for the client.
func (c *Client) WithDialConfig(dc amqp.Config) *Client {
	c.dialconfig = dc

	return c
}

// WithDialTimeout sets the DialTimeout and handshake deadlines to timeout.
func (c *Client) WithDialTimeout(timeout time.Duration) *Client {
	c.dialconfig.Dial = amqp.DefaultDial(timeout)

	return c
}

// WithTLS sets the TLS config in the dial config for the client.
func (c *Client) WithTLS(tlsConfig *tls.Config) *Client {
	c.dialconfig.TLSClientConfig = tlsConfig

	return c
}

// WithLogger sets the logger to use for error and debug logging. By default
// the library will log errors using the logger from [slog.Default]. Some logs
// will contain data contained in a [amqp.Delivery] or [amqp.Publishing],
// including any headers. If you want to avoid logging some of the fields you
// can use an [slog.Handler] to filter out the fields you don't want to log.
func (c *Client) WithLogger(logger *slog.Logger) *Client {
	c.logger = logger.With("component", "amqprpc-client")

	return c
}

// WithReplyToQueueDeclareArgs will set the settings used when declaring the
// reply-to queue.
func (c *Client) WithReplyToQueueDeclareArgs(args amqp.Table) *Client {
	c.replyToQueueDeclareArgs = args

	return c
}

// WithReplyToConsumerArgs will set the consumer args used when the client
// starts its reply-to consumer.
func (c *Client) WithReplyToConsumerArgs(args amqp.Table) *Client {
	c.replyToConsumerArgs = args

	return c
}

// WithConfirmMode sets the confirm-mode on the client. This causes the client
// to wait for confirmations, and if none arrives or the confirmation is marked
// as Nack, Client#Send() returns a corresponding error.
func (c *Client) WithConfirmMode(confirmMode bool) *Client {
	c.confirmMode = confirmMode

	return c
}

// WithTimeout will set the client timeout used when publishing messages.
// t will be rounded using the duration's Round function to the nearest
// multiple of a millisecond. Rounding will be away from zero.
func (c *Client) WithTimeout(t time.Duration) *Client {
	c.timeout = t.Round(time.Millisecond)

	return c
}

// WithMaxRetries sets the maximum amount of times the client will retry
// sending the request before giving up and returning the error to the caller
// of c.Send(). This retry will persist during reconnects.
func (c *Client) WithMaxRetries(n int) *Client {
	c.maxRetries = n

	return c
}

// AddMiddleware will add a middleware which will be executed on request.
func (c *Client) AddMiddleware(m ClientMiddlewareFunc) *Client {
	c.middlewares = append(c.middlewares, m)

	return c
}

func (c *Client) setDefaults() {
	c.replyToQueueDeclareArgs = amqp.Table{
		// Ensure the queue is deleted automatically when it's unused for
		// more than the set time. This is to ensure that messages that
		// are in flight during a reconnect doesn't get lost (which might
		// happen when using when using the auto-delete flag).
		"x-expires": 1 * 60 * 1000, // 1 minute.
	}

	c.confirmMode = true
}

// notifyOnErrorFuncs will notify everyone who listens to the [Client.OnError]
func (c *Client) notifyOnErrorFuncs(err error) {
	for _, onError := range c.onErrorFuncs {
		onError(err)
	}
}

// Connect will start the client and run it forever, reconnecting
// automatically when needed. Note that this will return as soon as the setup
// is started, to wait for the setup to be completed use [Client.OnConnected].
func (c *Client) Connect() {
	if !atomic.CompareAndSwapInt32(&c.isRunning, 0, 1) {
		// Already running.
		return
	}

	// Set the reply-to queue name to a unique name for this client and
	// instance. This ensures that we can re-connect and re-use the same queue
	// on any connection errors.
	c.replyToQueueName = fmt.Sprintf("%s.reply-to-%s", c.name, uuid.NewString())

	// Always assume that we don't want to stop initially.
	atomic.StoreInt32(&c.wantStop, 0)

	c.stopChan = make(chan struct{})
	c.didStopChan = make(chan struct{})

	go func() {
		for {
			c.logger.Debug("connecting...")

			err := c.runOnce()
			if err == nil {
				c.logger.Debug("finished gracefully")
				break
			}

			c.notifyOnErrorFuncs(err)

			if atomic.LoadInt32(&c.wantStop) == 1 {
				c.logger.Error("finished with error",
					slog.Any("error", err),
				)

				break
			}

			c.logger.Error("got error: will reconnect",
				slog.Any("error", err),
				slog.String("eta", "0.5s"),
			)

			time.Sleep(500 * time.Millisecond)
		}

		// Tell c.Close() that we have finished shutdown and that it can return.
		close(c.didStopChan)

		// Ensure we can start again.
		atomic.StoreInt32(&c.isRunning, 0)
	}()
}

// runOnce will connect amqp, setup all the amqp channels, run the publisher
// and run the replies consumer. The method will also return the underlying
// amqp error if the underlying connection or socket isn't gracefully closed.
// It will also block until the connection is gone.
func (c *Client) runOnce() error {
	c.logger.Debug("starting up...")

	inputConn, outputConn, err := createConnections(c.url, c.name, c.dialconfig)
	if err != nil {
		return errors.Join(err, ErrConnectFailed)
	}

	defer inputConn.Close()
	defer outputConn.Close()

	inputCh, outputCh, err := createChannels(inputConn, outputConn)
	if err != nil {
		return errors.Join(err, ErrConnectFailed)
	}

	defer inputCh.Close()
	defer outputCh.Close()

	// Start listening on NotifyClose before we properly begin so that we avoid
	// race when the consumer or publisher starts work before we call
	// monitorAndWait. All have a buffer of 1 as recommended by amqp-go.
	notifyInputConnClose := inputConn.NotifyClose(make(chan *amqp.Error, 1))
	notifyOutputConnClose := outputConn.NotifyClose(make(chan *amqp.Error, 1))
	notifyInputChClose := inputCh.NotifyClose(make(chan *amqp.Error, 1))
	notifyOutputChClose := outputCh.NotifyClose(make(chan *amqp.Error, 1))

	// We will wait on this to ensure that all go routines are done before we
	// exit this function.
	wg := sync.WaitGroup{}

	repliesConsumerTag, err := c.runRepliesConsumer(inputCh, &wg)
	if err != nil {
		return errors.Join(err, ErrConsumerStartFailed)
	}

	if c.confirmMode {
		// ConfirmMode is wanted, tell the amqp-server that we want to enable
		// confirm-mode on this channel and start the confirms consumer.
		err = outputCh.Confirm(
			false, // no-wait.
		)
		if err != nil {
			return err
		}

		wg.Add(1) // Confirms consumer.

		go c.runConfirmsConsumer(
			outputCh.NotifyPublish(make(chan amqp.Confirmation)),
			outputCh.NotifyReturn(make(chan amqp.Return)),
			&wg,
		)
	}

	wg.Add(1) // Publisher.

	go c.runPublisher(outputCh, &wg)

	// Notify everyone that the client has started. Runs sequentially so there
	// isn't any race conditions when working with the connections or channels.
	for _, onConnected := range c.onConnectedFuncs {
		onConnected(inputConn, outputConn, inputCh, outputCh)
	}

	_, err = monitorAndWait(
		make(chan struct{}),
		c.stopChan,
		notifyInputConnClose,
		notifyOutputConnClose,
		notifyInputChClose,
		notifyOutputChClose,
	)
	if err != nil {
		// We don't have a graceful exit, just return the error.
		return err
	}

	// 1. Stop the publisher by closing the output channel. This also closes
	// the confirms consumer if it's running.
	outputCh.Close()

	// 2. Stop the replies consumer by canceling the consumer.
	err = inputCh.Cancel(repliesConsumerTag, false)
	if err != nil {
		return err
	}

	// 3. The consumer is stopped, we can now close the input channel.
	inputCh.Close()

	// 3. Wait for all the go routines to finish.
	wg.Wait()

	return nil
}

// runPublisher consumes messages from chan requests and publishes them on the
// amqp exchange. The method will stop consuming if the underlying amqp channel
// is closed for any reason, and when this happens the messages will be put back
// in chan requests unless we have retried to many times.
func (c *Client) runPublisher(ouputChan *amqp.Channel, wg *sync.WaitGroup) {
	defer wg.Done()

	onClose := ouputChan.NotifyClose(make(chan *amqp.Error, 1))

	c.logger.Debug("running publisher...")

	// Delivery tags always starts at 1 but we increase it before we do any
	// .Publish() on the channel.
	nextDeliveryTag := uint64(0)

	for {
		select {
		case <-onClose:
			// The channels for publishing responses was closed, once the
			// client has started again. This loop will be restarted.
			c.logger.Debug("publisher stopped after channel was closed")
			return
		case request := <-c.requests:
			// Set the ReplyTo if needed, or ensure it's empty if it's not.
			if request.Reply {
				request.Publishing.ReplyTo = c.replyToQueueName
			} else {
				request.Publishing.ReplyTo = ""
			}

			c.logger.DebugContext(request.Context, "publishing request",
				slog.String("correlation_id", request.Publishing.CorrelationId),
			)

			// Setup the delivery tag for this request.
			nextDeliveryTag++
			request.deliveryTag = nextDeliveryTag

			// Ensure the replies, returns and confirms consumers can get a hold
			// of this request once they come in.
			c.requestsMap.Set(request)

			err := ouputChan.PublishWithContext(
				context.Background(),
				request.Exchange,
				request.RoutingKey,
				request.Mandatory,
				false, // immediate. not supported by RabbitMQ.
				request.Publishing,
			)
			if err != nil {
				// Normally a Publish that results in an error will
				// automatically close the channel and connection. But if the
				// error occurs during a flush, that doesn't happen.
				ouputChan.Close()

				c.retryRequest(request, err)

				c.logger.ErrorContext(request.Context,
					"publisher stopped because of error",
					slog.Any("error", err),
					slogGroupFor("request", slogAttrsForRequest(request)),
				)

				return
			}

			if !c.confirmMode && !request.Reply {
				// Since we won't get a confirmation of this request and we
				// don't want to have a reply, just return nil to the caller.
				c.respondToRequest(request, nil, nil)
			}
		}
	}
}

// retryRequest will retry the provided request, unless the request already
// has been retried too many times. Then the provided error will be sent to the
// caller instead.
func (c *Client) retryRequest(request *Request, err error) {
	if request.numRetries >= c.maxRetries {
		// We have already retried too many times
		c.logger.ErrorContext(request.Context,
			"could not publish, giving up",
			slog.Any("error", err),
			slogGroupFor("request", slogAttrsForRequest(request)),
		)

		// Return whatever error .Publish returned to the caller.
		c.respondToRequest(request, nil, err)

		return
	}

	request.numRetries++

	go func() {
		c.logger.DebugContext(request.Context, "queuing request for retry",
			slog.Any("error", err),
			slogGroupFor("request", slogAttrsForRequest(request)),
		)

		select {
		case c.requests <- request:
		case <-request.Context.Done():
			c.logger.ErrorContext(request.Context, "canceled while waiting for retry",
				slog.Any("error", errors.Join(context.Cause(request.Context), err)),
				slogGroupFor("request", slogAttrsForRequest(request)),
			)
		}
	}()
}

// runConfirmsConsumer will consume both confirmations and returns and since
// returns always arrives before confirmations we want to finish handling any
// return before we handle any confirmations.
func (c *Client) runConfirmsConsumer(confirms chan amqp.Confirmation, returns chan amqp.Return, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case ret, ok := <-returns:
			if !ok {
				// Only the closing of confirms will exit this loop. Closing
				// the returns channel will do nothing. This ensures that we
				// don't exit until we have handled all confirmations.
				returns = nil
				continue
			}

			request, ok := c.requestsMap.GetByCorrelationID(ret.CorrelationId)
			if !ok {
				// This could happen if we stop waiting for requests to return due
				// to a timeout. But since returns are normally very fast that
				// would mean that something isn't quite right on the amqp server.
				// Note: We use LogAttrs here because .Error takes a variadic
				// []any slice.
				c.logger.LogAttrs(
					context.Background(),
					slog.LevelError,
					"got return for unknown request",
					slogAttrsForReturn(&ret)...,
				)

				continue
			}

			c.logger.DebugContext(request.Context, "publishing is returned by server",
				slog.String("correlation_id", ret.CorrelationId),
			)

			request.returned = &ret

		case confirm, ok := <-confirms:
			if !ok {
				return
			}

			request, ok := c.requestsMap.GetByDeliveryTag(confirm.DeliveryTag)
			if !ok {
				// This could happen if we stop waiting for requests to return due
				// to a timeout. But since confirmations are normally very fast that
				// would mean that something isn't quite right on the amqp server.
				// Unfortunately there isn't any way of getting more information
				// than the delivery tag from a confirmation.
				c.logger.Error(
					"got confirmation of unknown request",
					slog.Uint64("delivery_tag", confirm.DeliveryTag),
				)

				continue
			}

			c.logger.DebugContext(request.Context, "confirming request",
				slog.String("correlation_id", request.Publishing.CorrelationId),
			)

			if !confirm.Ack {
				c.respondToRequest(request, nil, ErrRequestRejected)

				// Doesn't matter if the request wants the nil reply below because
				// we gave it an error instead.
				continue
			}

			// Check if the request was also returned.
			if request.returned != nil {
				c.respondToRequest(request, nil, fmt.Errorf("%w: %d, %s",
					ErrRequestReturned,
					request.returned.ReplyCode,
					request.returned.ReplyText,
				))

				continue
			}

			if !request.Reply {
				// The request isn't expecting a reply so we need give a nil
				// response instead to signal that we're done.
				c.respondToRequest(request, nil, nil)
			}
		}
	}
}

// respondToRequest will return the provided response to the caller.
func (c *Client) respondToRequest(request *Request, d *amqp.Delivery, err error) {
	select {
	case request.response <- response{delivery: d, err: err}:
		return
	case <-request.Context.Done():
		c.logger.ErrorContext(request.Context,
			"nobody is waiting for response",
			slog.Any("error", errors.Join(context.Cause(request.Context), err)),
			slogGroupFor("request", slogAttrsForRequest(request)),
			slogGroupFor("delivery", slogAttrsForDelivery(d)),
		)
	}
}

// runRepliesConsumer will declare and start consuming from the queue where we
// expect replies to come back. The method will stop consuming if the
// underlying amqp channel is closed for any reason.
func (c *Client) runRepliesConsumer(inChan *amqp.Channel, wg *sync.WaitGroup) (consumerTag string, err error) {
	// RabbitMQ will soon no longer support what they call "non-exclusive
	// transient queues". We want to support reconnects and so we cannot set
	// the exclusive flag since that would delete the queue on automatically on disconnect.
	// And so we must also set the durable flag to true.
	queue, err := inChan.QueueDeclare(
		c.replyToQueueName,
		true,  // Durable needs to be true since we must be non-exclusive.
		false, // Auto-delete, instead we set x-expires per default.
		false, // Exclusive must be false since we must support reconnects.
		false, // no-wait.
		c.replyToQueueDeclareArgs,
	)
	if err != nil {
		return "", err
	}

	tag := uuid.NewString()

	messages, err := inChan.Consume(
		queue.Name,
		tag,   // consumer tag.
		true,  // auto-ack. We don't support manual ack for the reply-to queue.
		true,  // exclusive. We must be the only consumer.
		false, // no-local.
		false, // no-wait.
		c.replyToConsumerArgs,
	)
	if err != nil {
		return "", err
	}

	wg.Add(1)

	go func() {
		defer wg.Done()

		c.logger.Debug("running replies consumer...")

		for response := range messages {
			request, ok := c.requestsMap.GetByCorrelationID(response.CorrelationId)
			if !ok {
				// The request has probably timed out between when it was in the
				// queue and when the user stopped waiting for it. We can safely
				// log this as a debug message.
				c.logger.Debug(
					"could not find where to reply",
					slogGroupFor("response", slogAttrsForDelivery(&response)),
				)

				continue
			}

			c.logger.DebugContext(request.Context, "forwarding reply",
				slog.String("correlation_id", response.CorrelationId),
			)

			c.respondToRequest(request, &response, nil)
		}

		c.logger.Debug("replies consumer is done")
	}()

	return tag, nil
}

// Send will send a Request by using a amqp.Publishing.
func (c *Client) Send(r *Request) (*amqp.Delivery, error) {
	//nolint:gocritic // We don't want to overwrite any slice so it's
	// intentional to store append result in new slice.
	middlewares := append(c.middlewares, r.middlewares...)

	return ClientMiddlewareChain(c.Sender, middlewares...)(r)
}

func (c *Client) send(r *Request) (*amqp.Delivery, error) {
	// Ensure that the publisher is running.
	c.Connect()

	// This is where we get the responses back.
	// If this request doesn't want a reply back (by setting Reply to false)
	// this channel will get a nil message after publisher has Published the
	// message.
	r.response = make(chan response)

	// Set the correlation id on the publishing if not yet set.
	if r.Publishing.CorrelationId == "" {
		r.Publishing.CorrelationId = uuid.New().String()
	}

	defer c.requestsMap.Delete(r)

	cancel := r.startTimeout(c.timeout)
	defer cancel()

	c.logger.Debug("queuing request", slog.String("correlation_id", r.Publishing.CorrelationId))

	select {
	case c.requests <- r:
		// successful send.
	case <-r.Context.Done():
		err := context.Cause(r.Context)

		c.logger.DebugContext(r.Context,
			"canceled while waiting for request queue",
			slog.Any("error", err),
			slog.String("correlation_id", r.Publishing.CorrelationId),
		)

		return nil, fmt.Errorf("%w while waiting for request queue", err)
	}

	c.logger.DebugContext(r.Context,
		"waiting for reply",
		slog.String("correlation_id", r.Publishing.CorrelationId),
	)

	select {
	case res := <-r.response:
		return res.delivery, res.err
	case <-r.Context.Done():
		err := context.Cause(r.Context)

		c.logger.DebugContext(r.Context,
			"canceled while waiting for response",
			slog.Any("error", err),
			slog.String("correlation_id", r.Publishing.CorrelationId),
		)

		return nil, fmt.Errorf("%w while waiting for response", err)
	}
}

// Stop will gracefully disconnect from AMQP. It is not guaranteed that all
// in flight requests or responses are handled before the disconnect. Instead
// the user should ensure that all calls to c.Send() has returned before calling
// c.Stop().
func (c *Client) Stop() {
	if atomic.LoadInt32(&c.isRunning) != 1 {
		return
	}

	atomic.StoreInt32(&c.wantStop, 1)

	close(c.stopChan)
	<-c.didStopChan
}
