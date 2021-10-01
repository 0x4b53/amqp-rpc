package amqprpc

import (
	"context"
	"fmt"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func traceServerMiddleware(id int) ServerMiddlewareFunc {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
			fmt.Fprint(rw, id)
			next(ctx, rw, d)
			fmt.Fprint(rw, id)
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

	unevenHandler := ServerMiddlewareChain(
		func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
			fmt.Fprint(rw, "Y")
		},
		traceServerMiddleware(1),
		traceServerMiddleware(2),
		traceServerMiddleware(3),
	)

	rWriter := &ResponseWriter{
		Publishing: &amqp.Publishing{},
	}

	handler(context.Background(), rWriter, amqp.Delivery{})
	assert.Equal(t, "1234X4321", string(rWriter.Publishing.Body), "middlewares are called in correct order")

	unevenHandler(context.Background(), rWriter, amqp.Delivery{})
	assert.Equal(t, "1234X4321123Y321", string(rWriter.Publishing.Body), "all middlewares are called")
}
