package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

func traceMiddleware(ID int) MiddlewareFunc {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
			fmt.Fprint(rw, ID)
			next(ctx, rw, d)
			fmt.Fprint(rw, ID)
		}
	}
}

func TestMiddlewareChain(t *testing.T) {
	handler := MiddlewareChain(
		func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
			fmt.Fprint(rw, "X")
		},
		traceMiddleware(1),
		traceMiddleware(2),
		traceMiddleware(3),
		traceMiddleware(4),
	)

	rWriter := &ResponseWriter{
		publishing: &amqp.Publishing{},
	}

	handler(context.Background(), rWriter, amqp.Delivery{})

	Equal(t, string(rWriter.Publishing().Body), "1234X4321")
}
