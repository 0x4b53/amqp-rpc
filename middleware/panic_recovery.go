package middleware

import (
	"context"

	"github.com/streadway/amqp"

	amqprpc "github.com/0x4b53/amqp-rpc"
)

// PanicRecovery is a middleware that will recover any panics caused by a
// handler down the middleware chain. If a panic happens the onRecovery func
// will be called with the return value from recover().
func PanicRecovery(onRecovery func(interface{}, context.Context, *amqprpc.ResponseWriter, amqp.Delivery)) amqprpc.ServerMiddlewareFunc {
	return func(next amqprpc.HandlerFunc) amqprpc.HandlerFunc {
		return func(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
			defer func() {
				if r := recover(); r != nil {
					onRecovery(r, ctx, rw, d)
				}
			}()

			next(ctx, rw, d)
		}
	}
}

// PanicRecoveryLogging is a middleware that will recover any panics caused by
// a handler down the middleware chain. If a panic happens the value will be
// logged to the provided logFunc.
func PanicRecoveryLogging(logFunc amqprpc.LogFunc) amqprpc.ServerMiddlewareFunc {
	return PanicRecovery(func(r interface{}, _ context.Context, _ *amqprpc.ResponseWriter, d amqp.Delivery) {
		logFunc("recovered (%s): %v", d.CorrelationId, r)
	})
}
