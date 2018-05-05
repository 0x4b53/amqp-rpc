package client

import (
	"context"

	"github.com/streadway/amqp"
)

// Request is a requet to perform with the client
type Request struct {
	Header     amqp.Table
	Body       []byte
	Context    context.Context
	RoutingKey string
	Reply      bool
}

// NewRequest will generate a new request to be published.
// The default request will add a Background context and always
// wait for reply.
func NewRequest(rk string) *Request {
	r := Request{
		RoutingKey: rk,
		Header: amqp.Table{
			"ContentType": "text/plain",
		},
		Reply:   true,
		Context: context.Background(),
	}

	return &r
}

// WithContext will add a context to the request.
func (r *Request) WithContext(c context.Context) *Request {
	r.Context = c

	return r
}

// WithResponse sets the value determening wether the request
// should wait for a response or not.
// A request that does not require a response will only catch errors
// occuring before the reuqest has been published.
func (r *Request) WithResponse(wr bool) *Request {
	r.Reply = wr

	return r
}

// WithContentType will update the content type passed in the header
// of the request. This value will bee set as the ContentType in
// the amqp.Publishing type but also preserved as a header value.
func (r *Request) WithContentType(ct string) *Request {
	r.Header["ContentType"] = ct

	return r
}

// WithBody sets the body used for the request.
func (r *Request) WithBody(b []byte) *Request {
	r.Body = b

	return r
}

// WithStringBody will convert a string to a byte slice and add as the
// body passed for the request.
func (r *Request) WithStringBody(b string) *Request {
	r.Body = []byte(b)

	return r
}
