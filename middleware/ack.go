package middleware

import (
	"context"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	amqprpc "github.com/0x4b53/amqp-rpc/v5"
)

// OnErrFunc is the function that will be called when the middleware get an
// error from `Ack`. The error and the delivery will be passed.
type OnErrFunc func(err error, delivery amqp.Delivery)

// OnAckErrorLog is a built-in function that will log the error if any is
// returned from `Ack`.
//
//	middleware := AckDelivery(OnAckErrorLog(log.Printf))
func OnAckErrorLog(logger *slog.Logger) OnErrFunc {
	return func(err error, delivery amqp.Delivery) {
		logger.Error("could not ack delivery (%s): %v\n",
			slog.Any("error", err),
			slog.String("correlation_id", delivery.CorrelationId),
		)
	}
}

// OnAckErrorSendOnChannel will first log the error and correlation ID and then
// try to send on the passed channel. If no one is consuming on the passed
// channel the middleware will not block but instead log a message about missing
// channel consumers.
func OnAckErrorSendOnChannel(logger *slog.Logger, ch chan struct{}) OnErrFunc {
	logErr := OnAckErrorLog(logger)

	return func(err error, delivery amqp.Delivery) {
		logErr(err, delivery)

		select {
		case ch <- struct{}{}:
		default:
			logger.Error("ack middleware: could not send on channel, no one is consuming",
				slog.String("correlation_id", delivery.CorrelationId),
			)
		}
	}
}

// AckDelivery is a middleware that will acknowledge the delivery after the
// handler has been executed. If the Ack fails the error and the `amqp.Delivery`
// will be passed to the `OnErrFunc`.
func AckDelivery(onErrFn OnErrFunc) amqprpc.ServerMiddlewareFunc {
	return func(next amqprpc.HandlerFunc) amqprpc.HandlerFunc {
		return func(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
			acknowledger := amqprpc.NewAwareAcknowledger(d.Acknowledger)
			d.Acknowledger = acknowledger

			next(ctx, rw, d)

			if acknowledger.Handled {
				return
			}

			if err := d.Ack(false); err != nil {
				onErrFn(err, d)
			}
		}
	}
}
