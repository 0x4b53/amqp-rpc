package middleware

import (
	"context"
	"log"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"

	amqprpc "github.com/0x4b53/amqp-rpc"
)

func TestAckDelivery(t *testing.T) {
	tests := []struct {
		name    string
		handler amqprpc.HandlerFunc
	}{
		{
			name:    "handler doesn't ack",
			handler: func(_ context.Context, _ *amqprpc.ResponseWriter, _ amqp.Delivery) {},
		},
		{
			name: "handler does ack",
			handler: func(_ context.Context, _ *amqprpc.ResponseWriter, d amqp.Delivery) {
				_ = d.Ack(false)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acknowledger := &amqprpc.MockAcknowledger{}

			handler := AckDelivery(log.Printf)(tt.handler)

			rw := amqprpc.NewResponseWriter(&amqp.Publishing{})
			d := amqp.Delivery{Acknowledger: acknowledger}

			handler(context.Background(), rw, d)

			assert.Equal(t, 1, acknowledger.Acks)
		})
	}
}
