package client

import "github.com/streadway/amqp"

// SendFunc represents the function that Send does. It takes a Request as input
// and returns a delivery and an error.
type SendFunc func(r *Request) (d *amqp.Delivery, e error)

// MiddlewareFunc represents a function that can be used as a middleware.
type MiddlewareFunc func(next SendFunc) SendFunc

// MiddlewareChain will attatch all given middlewares to your SendFunc.
// The middlewares will be executed in the same order as your input.
func MiddlewareChain(next SendFunc, m ...MiddlewareFunc) SendFunc {
	if len(m) == 0 {
		return next
	}

	return m[0](MiddlewareChain(next, m[1:cap(m)]...))
}
