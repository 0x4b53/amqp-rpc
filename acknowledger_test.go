package amqprpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAwareAcknowledger(t *testing.T) {
	var (
		ma  = &MockAcknowledger{}
		aac = NewAwareAcknowledger(ma)
	)

	// 1
	require.NoError(t, aac.Ack(1, false))

	assert.True(t, aac.Handled, "delivery handled")
	assert.Equal(t, 1, ma.Acks, "1 delivery processed")

	aac.Handled = false

	// 2
	require.NoError(t, aac.Nack(2, false, false))

	assert.True(t, aac.Handled, "delivery handled")
	assert.Equal(t, 1, ma.Nacks, "1 delivery processed")

	aac.Handled = false

	// 3
	require.NoError(t, aac.Reject(3, false))

	assert.True(t, aac.Handled, "delivery handled")
	assert.Equal(t, 1, ma.Rejects, "1 delivery rejected")

	aac.Handled = false
}
