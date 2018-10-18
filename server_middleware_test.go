package amqprpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
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

	unevenHandler := ServerMiddlewareChain(
		func(ctx context.Context, rw *ResponseWriter, d amqp.Delivery) {
			fmt.Fprint(rw, "Y")
		},
		traceServerMiddleware(1),
		traceServerMiddleware(2),
		traceServerMiddleware(3),
	)

	rWriter := &ResponseWriter{
		publishing: &amqp.Publishing{},
	}

	handler(context.Background(), rWriter, amqp.Delivery{})
	assert.Equal(t, "1234X4321", string(rWriter.Publishing().Body), "middlewares are called in correct order")

	unevenHandler(context.Background(), rWriter, amqp.Delivery{})
	assert.Equal(t, "1234X4321123Y321", string(rWriter.Publishing().Body), "all middlewares are called")
}
