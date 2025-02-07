package middleware

import (
	"context"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	amqprpc "github.com/0x4b53/amqp-rpc/v5"
)

// AckDelivery is a middleware that will acknowledge the delivery after the
// handler has been executed.
func AckDelivery() amqprpc.ServerMiddlewareFunc {
	return func(next amqprpc.HandlerFunc) amqprpc.HandlerFunc {
		return func(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
			next(ctx, rw, d)

			err := d.Ack(false)
			if err != nil {
				slog.ErrorContext(
					context.Background(),
					"acking message",
					"correlation_id",
					d.CorrelationId,
				)
			}
		}
	}
}
