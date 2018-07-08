package client

import (
	"github.com/streadway/amqp"
)

// Request is a requet to perform with the client
type Request struct {
	// Exchange is the exchange to which the rquest will be published when
	// passing it to the clients send function.
	Exchange string

	// Headers is the headers for the request. The headers is passed straight to
	// the amqp.Publishing type and is of the same type (amqp.Table)
	Headers amqp.Table

	// Body is the byte slice that will be sent as the body for the request.
	Body []byte

	// Routing key is the routing key that will be used in the amqp.Publishing
	// request.
	RoutingKey string

	// Reply is a boolean value telling if the request should wait for a reply
	// or just send the request without waiting.
	Reply bool
}

// NewRequest will generate a new request to be published. The default request
// will use the content type "text/plain" and always wait for reply.
func NewRequest(rk string) *Request {
	r := Request{
		RoutingKey: rk,
		Headers: amqp.Table{
			"ContentType": "text/plain",
		},
		Reply: true,
	}

	return &r
}

// WithExchange will set the exchange on to which the request will be published.
func (r *Request) WithExchange(e string) *Request {
	r.Exchange = e

	// Since we currently only support fanout exchanges as custom exchanges in
	// the amqp-rpc application the default value for reply will be set to false
	// if WithExchange is called.
	r.Reply = false

	return r
}

// WithHeaders will set the full amqp.Table as the headers for the request.
func (r *Request) WithHeaders(h amqp.Table) *Request {
	r.Headers = h

	return r
}

// WithResponse sets the value determening wether the request should wait for a
// response or not. A request that does not require a response will only catch
// errors occuring before the reuqest has been published.
func (r *Request) WithResponse(wr bool) *Request {
	r.Reply = wr

	return r
}

// WithContentType will update the content type passed in the header of the
// request. This value will bee set as the ContentType in the amqp.Publishing
// type but also preserved as a header value.
func (r *Request) WithContentType(ct string) *Request {
	r.Headers["ContentType"] = ct

	return r
}

// WithBody sets the body used for the request.
func (r *Request) WithBody(b []byte) *Request {
	r.Body = b

	return r
}

// WithStringBody will convert a string to a byte slice and add as the body
// passed for the request.
func (r *Request) WithStringBody(b string) *Request {
	r.Body = []byte(b)

	return r
}
