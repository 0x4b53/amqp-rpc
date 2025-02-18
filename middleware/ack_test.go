package middleware

import (
	"context"
	"sync/atomic"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	amqprpc "github.com/0x4b53/amqp-rpc/v5"
)

func TestAckDelivery(t *testing.T) {
	tests := []struct {
		handler   amqprpc.HandlerFunc
		name      string
		ackReturn error
	}{
		{
			name:      "handler doesn't ack",
			handler:   func(_ context.Context, _ *amqprpc.ResponseWriter, _ amqp.Delivery) {},
			ackReturn: nil,
		},
		{
			name: "handler does ack",
			handler: func(_ context.Context, _ *amqprpc.ResponseWriter, d amqp.Delivery) {
				_ = d.Ack(false)
			},
			ackReturn: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acknowledger := &amqprpc.MockAcknowledger{
				OnAckFn: func() error {
					return tt.ackReturn
				},
			}

			didSendOnCh := atomic.Bool{}
			didSendOnCh.Store(false)

			// We setup a channel to ensure we don't proceed until we started
			// the go routine that will listen to the signal.
			isListening := make(chan struct{})

			ch := make(chan struct{})
			go func() {
				close(isListening)
				<-ch
				didSendOnCh.Store(true)
			}()

			// Block until ready.
			<-isListening

			handler := AckDelivery(false)(tt.handler)

			rw := amqprpc.ResponseWriter{Publishing: &amqp.Publishing{}}
			d := amqp.Delivery{Acknowledger: acknowledger, CorrelationId: "id-1234"}

			handler(context.Background(), &rw, d)
		})
	}
}
