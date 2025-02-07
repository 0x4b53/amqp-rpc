package amqprpc

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcknowledgeHandler(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	require.NoError(t, err)

	inputCh, err := conn.Channel()
	require.NoError(t, err)

	var (
		ma  = &MockAcknowledger{}
		aac = NewSafeAcknowledgeHandler(ma, inputCh)
	)

	// 1
	require.NoError(t, aac.Ack(1, false))

	assert.True(t, aac.Handled, "delivery handled")
	assert.Equal(t, 1, ma.Acks, "1 delivery processed")

	// don't ack if already handled
	require.NoError(t, aac.Ack(1, false))
	assert.Equal(t, 1, ma.Acks, "1 delivery processed")

	aac.Handled = false

	// 2
	require.NoError(t, aac.Nack(2, false, false))

	assert.True(t, aac.Handled, "delivery handled")
	assert.Equal(t, 1, ma.Nacks, "1 delivery processed")

	// don't nack if already handled
	require.NoError(t, aac.Nack(2, false, false))
	assert.Equal(t, 1, ma.Nacks, "1 delivery processed")

	aac.Handled = false

	// 3
	require.NoError(t, aac.Reject(3, false))

	assert.True(t, aac.Handled, "delivery handled")
	assert.Equal(t, 1, ma.Rejects, "1 delivery rejected")

	// don't reject if already handled
	require.NoError(t, aac.Reject(3, false))
	assert.Equal(t, 1, ma.Rejects, "1 delivery processed")

	aac.Handled = false

	// connection has not been closed so far
	assert.False(t, inputCh.IsClosed())

	// close connection when ack fails
	ma.OnAckFn = func() error {
		return errors.New("ack error")
	}

	require.NoError(t, aac.Ack(1, false))

	assert.True(t, inputCh.IsClosed())
}
