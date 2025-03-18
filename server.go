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

// HandlerFunc is the function that handles all request based on the routing key.
type HandlerFunc func(context.Context, *ResponseWriter, amqp.Delivery)

// processedRequest is used to add the response from a handler func combined
// with a amqp.Delivery. The reason we need to combine those is that we reply
// to each request in a separate go routine and the delivery is required to
// determine on which queue to reply.
type processedRequest struct {
	replyTo    string
	publishing amqp.Publishing
}

// Server represents an AMQP server used within the RPC framework. The
// server uses bindings to keep a list of handler functions.
type Server struct {
	// url is the URL where the server should dial to start subscribing.
	url string

	// onConnectedFuncs will all be executed after the server has finished startup.
	onConnectedFuncs []OnConnectedFunc

	// onErrorFuncs will execute when an error occurs. For example inside
	// [Server.listenAndServe].
	onErrorFuncs []OnErrorFunc

	// bindings is a list of HandlerBinding that holds information about the
	// bindings and it's handlers.
	bindings []HandlerBinding

	// Exchanges is a list of exchanges that should be declared on startup.
	exchanges []ExchangeDeclareSettings

	// Name is the name of this server, it is used when creating connections,
	// queues and consumers.
	name string

	// middlewares are chained and executed on request.
	middlewares []ServerMiddlewareFunc

	// Every processed request will be responded to in a separate go routine.
	// The server holds a chanel on which all the responses from a handler func
	// is added.
	responses chan processedRequest

	// Dialconfig is a amqp.Config which holds information about the connection
	// such as authentication, TLS configuration, and a dialer which is a
	// function used to obtain a connection. By default the dialconfig will
	// include a dial function implemented in connection/dialer.go.
	dialconfig amqp.Config

	// stopChan channel is used to signal shutdowns when calling Stop(). The
	// channel will be closed when Stop() is called.
	stopChan chan struct{}

	// restartChan channel is used to signal restarts. It can be set by the user
	// so they can restart the server without having to call Stop()/Start()
	restartChan chan struct{}

	// isRunning is 1 when the server is running.
	isRunning int32

	// logger is the logger used for logging.
	logger *slog.Logger
}

// NewServer will return a pointer to a new Server.
func NewServer(url string) *Server {
	server := Server{
		url:         url,
		name:        "amqprpc-server",
		bindings:    []HandlerBinding{},
		middlewares: []ServerMiddlewareFunc{},
		dialconfig: amqp.Config{
			Dial: amqp.DefaultDial(2 * time.Second),
		},
		//nolint:revive // Keep variables for clarity
		// We ensure to always create a channel so we can call `Restart` without
		// blocking.
		restartChan: make(chan struct{}),
	}

	// use the standard logger default.
	server.WithLogger(slog.Default())

	return &server
}

// WithName sets the connection name prefix for the server. Every connection
// the server uses will have this prefix in it's connection name. The
// connection name is `myprefix.publisher` and `myprefix.consumer` for the
// publisher and consumer connection respectively. This can be overridden by
// using the `WithDialConfig` method to set the connection_name property.
func (s *Server) WithName(name string) *Server {
	s.name = name

	return s
}

// WithDialTimeout sets the DialTimeout and handshake deadline to timeout.
func (s *Server) WithDialTimeout(timeout time.Duration) *Server {
	s.dialconfig.Dial = amqp.DefaultDial(timeout)

	return s
}

// WithExchanges adds exchanges exchange to the list of exchanges that should
// be declared on startup.
func (s *Server) WithExchanges(exchanges ...ExchangeDeclareSettings) *Server {
	s.exchanges = append(s.exchanges, exchanges...)

	return s
}

// WithDialConfig sets the dial config used for the server.
func (s *Server) WithDialConfig(c amqp.Config) *Server {
	s.dialconfig = c

	return s
}

// WithTLS sets the TLS config in the dial config for the server.
func (s *Server) WithTLS(tlsConfig *tls.Config) *Server {
	s.dialconfig.TLSClientConfig = tlsConfig

	return s
}

// WithLogger sets the logger to use for error and debug logging. By default
// the library will log errors using the logger from [slog.Default]. Some logs
// will contain data contained in a [amqp.Delivery] or [amqp.Publishing],
// including any headers. If you want to avoid logging some of the fields you
// can use an [slog.Handler] to filter out the fields you don't want to log.
func (s *Server) WithLogger(logger *slog.Logger) *Server {
	s.logger = logger.With("component", "amqprpc-server")

	return s
}

// WithRestartChan will add a channel to the server that will trigger a restart
// when it's triggered.
func (s *Server) WithRestartChan(ch chan struct{}) *Server {
	s.restartChan = ch

	return s
}

// AddMiddleware will add a ServerMiddleware to the list of middlewares to be
// triggered before the handle func for each request.
func (s *Server) AddMiddleware(m ServerMiddlewareFunc) *Server {
	s.middlewares = append(s.middlewares, m)

	return s
}

// OnConnected can be used to hook into the connections/channels that the server
// is using. This can be useful if you want more control over amqp directly. Note
// that this is blocking and the server won't continue it's startup until this
// function has finished executing.
//
//	server := NewServer(url)
//	server.OnConnected(func(inConn, outConn *amqp.Connection, inChan, outChan *amqp.Channel) {
//	// Do something with amqp connections/channels.
//	})
func (s *Server) OnConnected(f OnConnectedFunc) {
	s.onConnectedFuncs = append(s.onConnectedFuncs, f)
}

// OnError can be used to hook into the errors that the server encounters.
func (s *Server) OnError(f OnErrorFunc) {
	s.onErrorFuncs = append(s.onErrorFuncs, f)
}

// notifyOnErrorFuncs will notify everyone who listens to the [Server.OnError]
func (s *Server) notifyOnErrorFuncs(err error) {
	for _, onError := range s.onErrorFuncs {
		onError(err)
	}
}

// notifyStarted will notify everyone who listens to the [Server.OnConnected]
// event. Runs sequentially so there isn't any race conditions when working
// with the connections or channels.
func (s *Server) notifyStarted(inputConn, outputConn *amqp.Connection, inputCh, outputCh *amqp.Channel) {
	for _, onConnected := range s.onConnectedFuncs {
		onConnected(inputConn, outputConn, inputCh, outputCh)
	}
}

// Bind will add a HandlerBinding to the list of servers to serve.
func (s *Server) Bind(binding HandlerBinding) {
	s.bindings = append(s.bindings, binding)
}

// ListenAndServe will dial the RabbitMQ message bus, set up all the channels,
// consume from all RPC server queues and monitor to connection to ensure the
// server is always connected.
func (s *Server) ListenAndServe() {
	s.responses = make(chan processedRequest)
	s.stopChan = make(chan struct{}) // Ensure .Stop() can use it.

	if !atomic.CompareAndSwapInt32(&s.isRunning, 0, 1) {
		// Already running.
		panic("Server is already running.")
	}

	for {
		shouldRestart, err := s.listenAndServe()
		// If we couldn't run listenAndServe and an error was returned, make
		// sure to check if the stopChan was closed - a user might know about
		// connection problems and have call Stop(). If the channel isn't
		// read/closed within 500ms, retry.
		if err != nil {
			s.notifyOnErrorFuncs(err)

			s.logger.Error("got error, will reconnect",
				"error", err,
				"eta", "0.5s",
			)

			select {
			case _, ok := <-s.stopChan:
				if !ok {
					s.logger.Debug("the stopChan was triggered in a reconnect loop, exiting")
					break
				}
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}

		if shouldRestart {
			// We must set up responses again. It's required to close to shut
			// down the responders so we let the shutdown process close it and
			// then we re-create it here.
			s.responses = make(chan processedRequest)

			s.logger.Debug("restarting listener")

			continue
		}

		s.logger.Debug("listener exiting gracefully")

		break
	}

	atomic.StoreInt32(&s.isRunning, 0)
}

func (s *Server) listenAndServe() (bool, error) {
	s.logger.Debug("starting listener", slog.String("url", s.url))

	// We are using two different connections here because:
	// "It's advisable to use separate connections for Channel.Publish and
	// Channel.Consume so not to have TCP pushback on publishing affect the
	// ability to consume messages [...]"
	// -- https://godoc.org/github.com/rabbitmq/amqp091-go#Channel.Consume
	inputConn, outputConn, err := createConnections(s.url, s.name, s.dialconfig)
	if err != nil {
		return false, errors.Join(err, ErrConnectFailed)
	}

	defer inputConn.Close()
	defer outputConn.Close()

	inputCh, outputCh, err := createChannels(inputConn, outputConn)
	if err != nil {
		return false, errors.Join(err, ErrConnectFailed)
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

	// Create any exchanges that must be declared on startup.
	err = createExchanges(inputCh, s.exchanges)
	if err != nil {
		return false, errors.Join(err, ErrExchangeCreateFailed)
	}

	// Setup a WaitGroup for use by consume(). This WaitGroup will be 0
	// when all consumers are finished consuming messages.
	consumersWg := sync.WaitGroup{}
	consumersWg.Add(1) // Sync the waitgroup to this goroutine.

	// consumerTags is used when we later want to tell AMQP that we want to
	// cancel our consumers.
	consumerTags, err := s.startConsumers(inputCh, &consumersWg)
	if err != nil {
		return false, errors.Join(err, ErrConsumerStartFailed)
	}

	// This WaitGroup will reach 0 when the responder() has finished sending
	// all responses.
	responderWg := sync.WaitGroup{}
	responderWg.Add(1) // Sync the waitgroup to this goroutine.

	go s.responder(outputCh, &responderWg)

	// Notify everyone that the server has started.
	s.notifyStarted(inputConn, outputConn, inputCh, outputCh)

	shouldRestart, err := monitorAndWait(
		s.restartChan,
		s.stopChan,
		notifyInputConnClose,
		notifyOutputConnClose,
		notifyInputChClose,
		notifyOutputChClose,
	)
	if err != nil {
		return shouldRestart, err
	}

	if shouldRestart {
		s.logger.Debug("restarting server")
	} else {
		s.logger.Debug("gracefully shutting down")
	}

	// 1. Tell amqp we want to shut down by canceling all the consumers.
	err = cancelConsumers(inputCh, consumerTags)
	if err != nil {
		return shouldRestart, err
	}

	// 3. We've told amqp to stop delivering messages, now we wait for all
	// the consumers to finish inflight messages.
	consumersWg.Done()
	consumersWg.Wait()

	// 4. Close the responses chan and wait until the consumers are finished.
	// We might still have responses we want to send.
	close(s.responses)
	responderWg.Done()
	responderWg.Wait()

	// 5. We have no more messages incoming and we've published all our
	// responses. The closing of connections and channels are deferred so we can
	// just return now.
	return shouldRestart, nil
}

func (s *Server) startConsumers(inputCh *amqp.Channel, wg *sync.WaitGroup) ([]string, error) {
	consumerTags := []string{}

	for _, binding := range s.bindings {
		consumerTag, err := s.consume(binding, inputCh, wg)
		if err != nil {
			return []string{}, err
		}

		consumerTags = append(consumerTags, consumerTag)
	}

	return consumerTags, nil
}

func (s *Server) consume(binding HandlerBinding, inputCh *amqp.Channel, wg *sync.WaitGroup) (string, error) {
	err := inputCh.Qos(
		binding.PrefetchCount,
		0,     // prefetch size.
		false, // global. We set it per consumer here.
	)
	if err != nil {
		return "", err
	}

	queueName, err := declareAndBind(inputCh, binding)
	if err != nil {
		return "", err
	}

	consumerTag := fmt.Sprintf("%s-%s-%s", s.name, queueName, uuid.NewString())

	deliveries, err := inputCh.Consume(
		queueName,
		consumerTag,
		binding.AutoAck,
		binding.ExclusiveConsumer, // Works only with classic queues.
		false,                     // no-local
		false,                     // no-wait.
		binding.ConsumerArgs,
	)
	if err != nil {
		return "", err
	}

	// Attach the middlewares to the handler.
	handler := ServerMiddlewareChain(binding.Handler, s.middlewares...)

	go s.runHandler(handler, deliveries, queueName, wg)

	return consumerTag, nil
}

func (s *Server) runHandler(
	handler HandlerFunc,
	deliveries <-chan amqp.Delivery,
	queueName string,
	wg *sync.WaitGroup,
) {
	wg.Add(1)
	defer wg.Done()

	s.logger.Debug(
		"waiting for messages on queue",
		slog.String("queue", queueName),
	)

	for delivery := range deliveries {
		// Add one delta to the wait group each time a delivery is handled so
		// we can end by marking it as done. This will ensure that we don't
		// close the responses channel until the very last go routing handling
		// a delivery is finished even though we handle them concurrently.
		wg.Add(1)

		s.logger.Debug(
			"got delivery",
			slog.String("queue", queueName),
			slog.String("correlation_id", delivery.CorrelationId),
		)

		rw := ResponseWriter{
			Publishing: &amqp.Publishing{
				CorrelationId: delivery.CorrelationId,
				Body:          []byte{},
			},
		}

		ctx := context.Background()
		ctx = ContextWithShutdownChan(ctx, s.stopChan)
		ctx = ContextWithQueueName(ctx, queueName)

		acknowledger := NewAcknowledger(delivery.Acknowledger)
		delivery.Acknowledger = acknowledger

		go func(delivery amqp.Delivery) {
			handler(ctx, &rw, delivery)

			if delivery.ReplyTo != "" {
				s.responses <- processedRequest{
					replyTo:    delivery.ReplyTo,
					publishing: *rw.Publishing,
				}
			}

			// Mark the specific delivery as finished.
			wg.Done()
		}(delivery)
	}

	s.logger.Debug(
		"stopped waiting for messages on queue",
		slog.String("queue", queueName),
	)
}

func (s *Server) responder(outCh *amqp.Channel, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	for response := range s.responses {
		s.logger.Debug(
			"publishing response",
			slog.String("reply_to", response.replyTo),
			slog.String("correlation_id", response.replyTo),
		)

		err := outCh.PublishWithContext(
			context.Background(),
			"", // Use the default exchange, publishes directly to the queue.
			response.replyTo,
			false, // mandatory. Don't fail if the client has stopped.
			false, // immediate. Not supported by RabbitMQ.
			response.publishing,
		)
		if err != nil {
			// Close the channel so ensure reconnect.
			outCh.Close()

			// We resend the response here so that other running goroutines
			// that have a working outCh can pick up this response.
			s.logger.Error(
				"retrying publishing response",
				slog.Any("error", err),
				slog.String("reply_to", response.replyTo),
				slogGroupFor("publishing", slogAttrsForPublishing(&response.publishing)),
			)

			s.responses <- response

			return
		}
	}
}

// Stop will gracefully disconnect from AMQP after draining first incoming then
// outgoing messages. This method won't wait for server shutdown to complete,
// you should instead wait for ListenAndServe to exit.
func (s *Server) Stop() {
	if atomic.LoadInt32(&s.isRunning) == 0 {
		return
	}

	close(s.stopChan)
}

// Restart will gracefully disconnect from AMQP exactly like `Stop` but instead
// of returning from `ListenAndServe` it will set everything up again from
// scratch and start listening again. This can be useful if a server restart is
// wanted without running `ListenAndServe` in a loop.
func (s *Server) Restart() {
	// Restart is noop if not running.
	if atomic.LoadInt32(&s.isRunning) == 0 {
		return
	}

	// Ensure we never block on the restartChan, if we're in the middle of a
	// setup or teardown process we won't be listening on this channel and if so
	// we do a noop.
	// This can likely happen e.g. if you have multiple messages in memory and
	// acknowledging them stops working, you might call `Restart` on all of them
	// but only the first one should trigger the restart.
	select {
	case s.restartChan <- struct{}{}:
	default:
		s.logger.Debug("no listener on restartChan, ensure server is running")
	}
}

// cancelConsumers will cancel the specified consumers.
func cancelConsumers(channel *amqp.Channel, consumerTags []string) error {
	for _, consumerTag := range consumerTags {
		err := channel.Cancel(consumerTag, false)
		if err != nil {
			return err
		}
	}

	return nil
}

// declareAndBind will declare a queue, an exchange and the queue to the
// exchange.
func declareAndBind(inputCh *amqp.Channel, binding HandlerBinding) (string, error) {
	queueName := binding.QueueName

	if !binding.SkipQueueDeclare {
		queue, err := inputCh.QueueDeclare(
			binding.QueueName,
			binding.QueueDurable,
			binding.QueueAutoDelete,
			binding.QueueExclusive,
			false, // no-wait.
			binding.QueueDeclareArgs,
		)
		if err != nil {
			return "", err
		}

		// In case of auto generated queue name.
		queueName = queue.Name
	}

	err := inputCh.QueueBind(
		queueName,
		binding.RoutingKey,
		binding.ExchangeName,
		false, // no-wait.
		binding.BindHeaders,
	)
	if err != nil {
		return "", err
	}

	return queueName, nil
}
