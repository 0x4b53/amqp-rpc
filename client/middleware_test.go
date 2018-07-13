package client

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

func traceMiddleware(ID int, b *bytes.Buffer) MiddlewareFunc {
	return func(next SendFunc) SendFunc {
		return func(r *Request) (*amqp.Delivery, error) {
			fmt.Fprint(b, ID)
			res, err := next(r)
			fmt.Fprint(b, ID)

			return res, err
		}
	}
}

func TestMiddlewareChain(t *testing.T) {
	var (
		req = NewRequest("")
		b   = bytes.Buffer{}
	)

	mw := MiddlewareChain(
		func(rx *Request) (*amqp.Delivery, error) {
			fmt.Fprintf(&b, "X")

			dx := amqp.Delivery{}

			return &dx, nil
		},
		traceMiddleware(1, &b),
		traceMiddleware(2, &b),
	)

	res, err := mw(req)

	Equal(t, err, nil)
	NotEqual(t, res, nil)
	Equal(t, b.Bytes(), []byte("12X21"))
}

func TestClientAddMiddlewares(t *testing.T) {
	c := New("")

	Equal(t, len(c.middlewares), 0)

	c.AddMiddleware(
		func(n SendFunc) SendFunc {
			return func(r *Request) (*amqp.Delivery, error) {
				return n(r)
			}
		},
	)

	Equal(t, len(c.middlewares), 1)
}
