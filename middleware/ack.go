package middleware

import (
	"context"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	amqprpc "github.com/0x4b53/amqp-rpc/v5"
)

// AckDelivery is a middleware that will acknowledge the delivery after the
// handler has been executed. If the Ack fails the error and the `amqp.Delivery`
// will be passed to the `OnErrFunc`.
func AckDelivery(logOnError bool) amqprpc.ServerMiddlewareFunc {
	return func(next amqprpc.HandlerFunc) amqprpc.HandlerFunc {
		return func(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
			next(ctx, rw, d)

			if err := d.Ack(false); err != nil && logOnError {
				slog.ErrorContext(ctx, "failed to ack message", slog.Any("error", err))
			}
		}
	}
}
