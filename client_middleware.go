package amqprpc

import (
	"github.com/streadway/amqp"
)

// SendFunc represents the function that Send does. It takes a Request as input
// and returns a delivery and an error.
type SendFunc func(r *Request) (d *amqp.Delivery, e error)

// ClientMiddlewareFunc represents a function that can be used as a middleware.
type ClientMiddlewareFunc func(next SendFunc) SendFunc

// ClientMiddlewareChain will attatch all given middlewares to your SendFunc.
// The middlewares will be executed in the same order as your input.
func ClientMiddlewareChain(next SendFunc, m ...ClientMiddlewareFunc) SendFunc {
	if len(m) == 0 {
		return next
	}

	return m[0](ClientMiddlewareChain(next, m[1:len(m)]...))
}
