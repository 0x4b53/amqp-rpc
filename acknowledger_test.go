package amqprpc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAwareAcknowledger(t *testing.T) {
	var (
		ma  = &MockAcknowledger{}
		aac = NewAcknowledger(ma)
	)

	// 1
	require.NoError(t, aac.Ack(1, false))

	assert.True(t, aac.Handled.Load(), "delivery handled")
	assert.Equal(t, 1, ma.Acks, "1 delivery processed")

	aac.Handled.Store(false)

	// 2
	require.NoError(t, aac.Nack(2, false, false))

	assert.True(t, aac.Handled.Load(), "delivery handled")
	assert.Equal(t, 1, ma.Nacks, "1 delivery processed")

	aac.Handled.Store(false)

	// 3
	require.NoError(t, aac.Reject(3, false))

	assert.True(t, aac.Handled.Load(), "delivery handled")
	assert.Equal(t, 1, ma.Rejects, "1 delivery rejected")

	// make sure the close function is called when ack fails
	aac.Handled.Store(false)

	ma.OnAckFn = func() error {
		return errors.New("ack error")
	}

	err := aac.Ack(1, false)
	require.Error(t, err)
	assert.Equal(t, "ack error", err.Error())

	assert.Equal(t, 1, ma.Closes)

	aac.Handled.Store(false)
}
