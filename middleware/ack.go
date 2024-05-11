package middleware

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	amqprpc "github.com/0x4b53/amqp-rpc/v3"
)

type OnErrFunc func(err error, correlationID string)

func AckLogError(err error, correlationID string) {
	log.Printf("could not ack delivery (%s): %v\n", correlationID, err)
}

// AckDelivery is a middleware that will acknowledge the delivery after the
// handler has been executed. Any error returned from d.Ack will be passed
// to the provided logFunc.
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
				onErrFn(err, d.CorrelationId)
			}
		}
	}
}
