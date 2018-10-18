package amqprpc

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
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
		assert = assert.New(t)
		req    = NewRequest()
		b      = bytes.Buffer{}
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

	assert.Nil(err, "no errors chaining middlewares")
	assert.NotNil(res, "result is not nil after passing through middlewares")
	assert.Equal([]byte("12X21"), b.Bytes(), "middlewares executed in correct order")
}

func TestClientClientAddMiddlewares(t *testing.T) {
	c := NewClient("")

	assert.Equal(t, 0, len(c.middlewares), "zero middlewares at start")

	c.AddMiddleware(
		func(n SendFunc) SendFunc {
			return func(r *Request) (*amqp.Delivery, error) {
				return n(r)
			}
		},
	)

	assert.Equal(t, 1, len(c.middlewares), "adding middlewares working")
}
