package amqprpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAwareAcknowledger(t *testing.T) {
	var (
		assert = assert.New(t)
		ma     = &MockAcknowledger{}
		aac    = NewAwareAcknowledger(ma)
	)

	// 1
	assert.Nil(aac.Ack(1, false))

	assert.Equal(true, aac.Handled, "delivery handled")
	assert.Equal(1, ma.Acks, "1 delivery processed")

	aac.Handled = false

	// 2
	assert.Nil(aac.Nack(2, false, false))

	assert.Equal(true, aac.Handled, "delivery handled")
	assert.Equal(1, ma.Nacks, "1 delivery processed")

	aac.Handled = false

	// 3
	assert.Nil(aac.Reject(3, false))

	assert.Equal(true, aac.Handled, "delivery handled")
	assert.Equal(1, ma.Rejects, "1 delivery rejected")

	aac.Handled = false
}
