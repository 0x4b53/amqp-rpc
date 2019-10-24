package middleware

import (
	"context"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"

	amqprpc "github.com/0x4b53/amqp-rpc"
)

func TestPanicRecovery(t *testing.T) {
	responseWriter := amqprpc.NewResponseWriter(&amqp.Publishing{})
	delivery := amqp.Delivery{}
	called := false

	onRecovery := func(r interface{}, _ context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
		assert.Equal(t, "oopsie!", r)
		assert.Equal(t, responseWriter, rw)
		assert.Equal(t, delivery, d)

		called = true
	}

	handler := PanicRecovery(onRecovery)(func(_ context.Context, _ *amqprpc.ResponseWriter, _ amqp.Delivery) {
		panic("oopsie!")
	})

	assert.NotPanics(t, func() {
		handler(context.Background(), responseWriter, delivery)
	})

	assert.True(t, called)
}
