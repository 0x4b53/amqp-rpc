package amqprpc

import (
	"context"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

// Request is a requet to perform with the client
type Request struct {
	// Exchange is the exchange to which the rquest will be published when
	// passing it to the clients send function.
	Exchange string

	// Routing key is the routing key that will be used in the amqp.Publishing
	// request.
	RoutingKey string

	// Reply is a boolean value telling if the request should wait for a reply
	// or just send the request without waiting.
	Reply bool

	// Timeout is the time we should wait after a request is sent before
	// we assume the request got lost.
	Timeout time.Duration

	// Publishing is the publising that are going to be sent.
	Publishing amqp.Publishing

	// Context is a context wich you can use to pass data from where the
	// request is created to middlewares. By default this will be a
	// context.Background()
	Context context.Context

	// sends a request. This is only executed for the specific request.
	middlewares []ClientMiddlewareFunc

	// These channels are used by the repliesConsumer and correlcationIdMapping
	// and will send the replies to this Request here. If we get a client error
	// (e.g we can't publish) it will end up on the ErrChan.
	response chan *amqp.Delivery
	errChan  chan error

	// the number of times that the publisher should retry.
	numRetries int

	// stream is a boolean telling if WithStream was used. By using this option
	// we won't read from the response channel but let the user do it by
	// calling Stream(). We also don't remove any mapping to a correlation ID,
	// this is done when the user calls EndStream.
	stream bool

	// closeFunc is the function to call when ending the stream. Since it's
	// created while sending a request the function already stores all local
	// variables required. The function set in the client will remove IDs from
	// the correlation ID mapping, close the response channel, set streaming to
	// false and reset the history of correlation IDs.
	closeFunc func()

	// correlationID is a history list of all correlation IDs generated (and
	// sent) for a given request. This is used to clear the correlation ID
	// mapping when the user is done with a streaming request.
	correlationIDs []string
}

// NewRequest will generate a new request to be published. The default request
// will use the content type "text/plain" and always wait for reply.
func NewRequest() *Request {
	r := Request{
		Context:     context.Background(),
		Reply:       true,
		middlewares: []ClientMiddlewareFunc{},
		Publishing: amqp.Publishing{
			ContentType: "text/plain",
			Headers:     amqp.Table{},
		},
	}

	return &r
}

// WithRoutingKey will set the routing key for the request.
func (r *Request) WithRoutingKey(rk string) *Request {
	r.RoutingKey = rk

	return r
}

// WithContext will set the context on the request.
func (r *Request) WithContext(ctx context.Context) *Request {
	r.Context = ctx

	return r
}

// WriteHeader will write a header for the specified key.
func (r *Request) WriteHeader(header string, value interface{}) {
	r.Publishing.Headers[header] = value
}

// WithExchange will set the exchange on to which the request will be published.
func (r *Request) WithExchange(e string) *Request {
	r.Exchange = e

	return r
}

// WithHeaders will set the full amqp.Table as the headers for the request.
// Note that this will overwrite anything previously set on the headers.
func (r *Request) WithHeaders(h amqp.Table) *Request {
	r.Publishing.Headers = h

	return r
}

// WithTimeout will set the client timeout used when publishing messages.
// t will be rounded using the duration's Round function to the nearest
// multiple of a millisecond. Rounding will be away from zero.
func (r *Request) WithTimeout(t time.Duration) *Request {
	r.Timeout = t.Round(time.Millisecond)

	return r
}

// WithResponse sets the value determining wether the request should wait for a
// response or not. A request that does not require a response will only catch
// errors occurring before the reuqest has been published.
func (r *Request) WithResponse(wr bool) *Request {
	r.Reply = wr

	return r
}

// WithContentType will update the content type passed in the header of the
// request. This value will bee set as the ContentType in the amqp.Publishing
// type but also preserved as a header value.
func (r *Request) WithContentType(ct string) *Request {
	r.Publishing.ContentType = ct

	return r
}

// WithBody will convert a string to a byte slice and add as the body
// passed for the request.
func (r *Request) WithBody(b string) *Request {
	r.Publishing.Body = []byte(b)

	return r
}

// WithStream will return the response channel in addition to the *Request.
// After this function is called the client will not close the channel on the
// first response but continue to stream responses on the channel until it's
// closed.
func (r *Request) WithStream() *Request {
	r.stream = true

	return r
}

// Stream returns the response channel which won't be closed when WithStream is
// used.
func (r *Request) Stream() chan *amqp.Delivery {
	return r.response
}

// EndStream will close the response channel on the request and remove all
// related mappings used for the channel.
func (r *Request) EndStream() {
	r.closeFunc()
}

// Write will write the response Body of the amqp.Publishing.
// It is safe to call Write multiple times.
func (r *Request) Write(p []byte) (int, error) {
	r.Publishing.Body = append(r.Publishing.Body, p...)

	return len(p), nil
}

// AddMiddleware will add a middleware which will be executed when the request
// is sent.
func (r *Request) AddMiddleware(m ClientMiddlewareFunc) *Request {
	r.middlewares = append(r.middlewares, m)

	return r
}

// StartTimeout will start the timeout counter by using Duration.After.
// Is will also set the Expiration field for the Publishing so that amqp won't
// hold on to the message in the queue after the timeout has happened.
func (r *Request) StartTimeout() <-chan time.Time {
	r.Publishing.Expiration = fmt.Sprintf("%d", r.Timeout.Nanoseconds()/1e6)

	return time.After(r.Timeout)
}
