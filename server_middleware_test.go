package amqprpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

func traceServerMiddleware(ID int) ServerMiddlewareFunc {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
			fmt.Fprint(rw, ID)
			next(ctx, rw, d)
			fmt.Fprint(rw, ID)
		}
	}
}

func TestServerMiddlewareChain(t *testing.T) {
	handler := ServerMiddlewareChain(
		func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
			fmt.Fprint(rw, "X")
		},
		traceServerMiddleware(1),
		traceServerMiddleware(2),
		traceServerMiddleware(3),
		traceServerMiddleware(4),
	)

	rWriter := &ResponseWriter{
		publishing: &amqp.Publishing{},
	}

	handler(context.Background(), rWriter, amqp.Delivery{})

	Equal(t, string(rWriter.Publishing().Body), "1234X4321")
}
