package amqprpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
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
	mandatory  bool
	immediate  bool
	publishing amqp.Publishing
}

// Server represents an AMQP server used within the RPC framework. The
// server uses bindings to keep a list of handler functions.
type Server struct {
	// url is the URL where the server should dial to start subscribing.
	url string

	// onStarteds will all be executed after the server has finished startup.
	onStarteds []OnStartedFunc

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

	// errorLog specifies an optional logger for amqp errors, unexpected behavior etc.
	// If nil, logging is done via the log package's standard logger.
	errorLog LogFunc

	// debugLog specifies an optional logger for debugging, this logger will
	// print most of what is happening internally.
	// If nil, logging is not done.
	debugLog LogFunc
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
		errorLog: log.Printf, // use the standard logger default.
		//nolint:revive // Keep variables for clarity
		debugLog: func(format string, args ...interface{}) {}, // don't print anything default.
		// We ensure to always create a channel so we can call `Restart` without
		// blocking.
		restartChan: make(chan struct{}),
	}

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

// WithErrorLogger sets the logger to use for error logging.
func (s *Server) WithErrorLogger(f LogFunc) *Server {
	s.errorLog = f

	return s
}

// WithDebugLogger sets the logger to use for debug logging.
func (s *Server) WithDebugLogger(f LogFunc) *Server {
	s.debugLog = f

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

/*
OnStarted can be used to hook into the connections/channels that the server is
using. This can be useful if you want more control over amqp directly. The
OnStartedFunc will be executed after ListenAndServe is called. Note that this
function is blocking and the server won't continue it's startup until it has
finished executing.

	server := NewServer(url)
	server.OnStarted(func(inConn, outConn *amqp.Connection, inChan, outChan *amqp.Channel) {
		// Do something with amqp connections/channels.
	})
*/
func (s *Server) OnStarted(f OnStartedFunc) {
	s.onStarteds = append(s.onStarteds, f)
}

// notifyStarted will notify everyone who listens to the OnStarted event.
// Runs sequentially so there isn't any race conditions when working with the
// connections or channels.
func (s *Server) notifyStarted(inputConn, outputConn *amqp.Connection, inputCh, outputCh *amqp.Channel) {
	for _, onStarted := range s.onStarteds {
		onStarted(inputConn, outputConn, inputCh, outputCh)
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
			s.errorLog("server: got error: %v, will reconnect in %v second(s)", err, 0.5)

			select {
			case _, ok := <-s.stopChan:
				if !ok {
					s.debugLog("server: the stopChan was triggered in a reconnect loop, exiting")
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

			s.debugLog("server: listener restarting")

			continue
		}

		s.debugLog("server: listener exiting gracefully")

		break
	}

	atomic.StoreInt32(&s.isRunning, 0)
}

func (s *Server) listenAndServe() (bool, error) {
	s.debugLog("server: starting listener: %s", s.url)

	// We are using two different connections here because:
	// "It's advisable to use separate connections for Channel.Publish and
	// Channel.Consume so not to have TCP pushback on publishing affect the
	// ability to consume messages [...]"
	// -- https://godoc.org/github.com/rabbitmq/amqp091-go#Channel.Consume
	inputConn, outputConn, err := createConnections(s.url, s.name, s.dialconfig)
	if err != nil {
		return false, err
	}

	defer inputConn.Close()
	defer outputConn.Close()

	inputCh, outputCh, err := createChannels(inputConn, outputConn)
	if err != nil {
		return false, err
	}

	defer inputCh.Close()
	defer outputCh.Close()

	// Notify everyone that the server has started.
	s.notifyStarted(inputConn, outputConn, inputCh, outputCh)

	// Create any exchanges that must be declared on startup.
	err = createExchanges(inputCh, s.exchanges)
	if err != nil {
		return false, err
	}

	// Setup a WaitGroup for use by consume(). This WaitGroup will be 0
	// when all consumers are finished consuming messages.
	consumersWg := sync.WaitGroup{}
	consumersWg.Add(1) // Sync the waitgroup to this goroutine.

	// consumerTags is used when we later want to tell AMQP that we want to
	// cancel our consumers.
	consumerTags, err := s.startConsumers(inputCh, &consumersWg)
	if err != nil {
		return false, err
	}

	// This WaitGroup will reach 0 when the responder() has finished sending
	// all responses.
	responderWg := sync.WaitGroup{}
	responderWg.Add(1) // Sync the waitgroup to this goroutine.

	go s.responder(outputCh, &responderWg)

	shouldRestart, err := monitorAndWait(
		s.restartChan,
		s.stopChan,
		inputConn.NotifyClose(make(chan *amqp.Error)),
		outputConn.NotifyClose(make(chan *amqp.Error)),
		inputCh.NotifyClose(make(chan *amqp.Error)),
		outputCh.NotifyClose(make(chan *amqp.Error)),
	)
	if err != nil {
		return shouldRestart, err
	}

	if shouldRestart {
		s.debugLog("server: restarting server")
	} else {
		s.debugLog("server: gracefully shutting down")
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
		false, // exclusive.
		false, // no-local
		false, // no-wait.
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

	s.debugLog("server: waiting for messages on queue '%s'", queueName)

	for delivery := range deliveries {
		// Add one delta to the wait group each time a delivery is handled so
		// we can end by marking it as done. This will ensure that we don't
		// close the responses channel until the very last go routing handling
		// a delivery is finished even though we handle them concurrently.
		wg.Add(1)

		s.debugLog("server: got delivery on queue %v correlation id %v", queueName, delivery.CorrelationId)

		rw := ResponseWriter{
			Publishing: &amqp.Publishing{
				CorrelationId: delivery.CorrelationId,
				Body:          []byte{},
			},
		}

		ctx := context.Background()
		ctx = ContextWithShutdownChan(ctx, s.stopChan)
		ctx = ContextWithQueueName(ctx, queueName)

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

	s.debugLog("server: stopped waiting for messages on queue '%s'", queueName)
}

func (s *Server) responder(outCh *amqp.Channel, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	for response := range s.responses {
		s.debugLog(
			"server: publishing response to %s, correlation id: %s",
			response.replyTo, response.publishing.CorrelationId,
		)

		err := outCh.PublishWithContext(
			context.Background(),
			"", // exchange
			response.replyTo,
			response.mandatory,
			response.immediate,
			response.publishing,
		)
		if err != nil {
			// Close the channel so ensure reconnect.
			outCh.Close()

			// We resend the response here so that other running goroutines
			// that have a working outCh can pick up this response.
			s.errorLog(
				"server: retrying publishing response to %s, reason: %s, response: %s",
				response.replyTo, err.Error(), stringifyPublishingForLog(response.publishing),
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
	// setup or tare-down process we won't be listening on this channel and if so
	// we do a noop.
	// This can likely happen e.g. if you have multiple messages in memory and
	// acknowledging them stops working, you might call `Restart` on all of them
	// but only the first one should trigger the restart.
	select {
	case s.restartChan <- struct{}{}:
	default:
		s.debugLog("server: no listener on restartChan, ensure server is running")
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
			binding.QueueArgs,
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
