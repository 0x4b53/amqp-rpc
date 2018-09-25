package amqprpc

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

func traceClientMiddleware(ID int, b *bytes.Buffer) ClientMiddlewareFunc {
	return func(next SendFunc) SendFunc {
		return func(r *Request) (*amqp.Delivery, error) {
			fmt.Fprint(b, ID)
			res, err := next(r)
			fmt.Fprint(b, ID)

			return res, err
		}
	}
}

func TestClientMiddlewareChain(t *testing.T) {
	var (
		req = NewRequest()
		b   = bytes.Buffer{}
	)

	mw := ClientMiddlewareChain(
		func(rx *Request) (*amqp.Delivery, error) {
			fmt.Fprintf(&b, "X")

			dx := amqp.Delivery{}

			return &dx, nil
		},
		traceClientMiddleware(1, &b),
		traceClientMiddleware(2, &b),
	)

	res, err := mw(req)

	Equal(t, err, nil)
	NotEqual(t, res, nil)
	Equal(t, b.Bytes(), []byte("12X21"))
}

func TestClientClientAddMiddlewares(t *testing.T) {
	c := NewClient("")

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
