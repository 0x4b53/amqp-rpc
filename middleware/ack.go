package middleware

import (
	"context"

	"github.com/streadway/amqp"

	amqprpc "github.com/0x4b53/amqp-rpc"
)

// AckDelivery is a middleware that will acknowledge the delivery after the
// handler has been executed. Any error returned from d.Ack will be passed
// to the provided logFunc.
func AckDelivery(logFunc amqprpc.LogFunc) amqprpc.ServerMiddlewareFunc {
	return func(next amqprpc.HandlerFunc) amqprpc.HandlerFunc {
		return func(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
			acknowledger := amqprpc.NewAwareAcknowledger(d.Acknowledger)
			d.Acknowledger = acknowledger

			next(ctx, rw, d)

			if acknowledger.Handled {
				return
			}

			err := d.Ack(false)
			if err != nil {
				logFunc("could not Ack delivery (%s): %v", d.CorrelationId, err)
			}
		}
	}
}
