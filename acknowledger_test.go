package amqprpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockAcknowledger struct {
	ack    int
	nack   int
	reject int
}

func (ma *mockAcknowledger) Ack(tag uint64, multiple bool) error {
	ma.ack++
	return nil
}

func (ma *mockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	ma.nack++
	return nil
}

func (ma *mockAcknowledger) Reject(tag uint64, requeue bool) error {
	ma.reject++
	return nil
}

func TestAckAwareChannel(t *testing.T) {
	var (
		assert = assert.New(t)
		ma     = mockAcknowledger{}
		aac    = ackAwareChannel{ch: &ma, handled: false}
	)

	// 1
	aac.Ack(1, false)

	assert.Equal(true, aac.handled, "delivery handled")
	assert.Equal(1, ma.ack, "1 delivery processed")

	aac.handled = false

	// 2
	aac.Nack(2, false, false)

	assert.Equal(true, aac.handled, "delivery handled")
	assert.Equal(1, ma.nack, "1 delivery processed")

	aac.handled = false

	// 3
	aac.Reject(3, false)

	assert.Equal(true, aac.handled, "delivery handled")
	assert.Equal(1, ma.reject, "1 delivery rejected")

	aac.handled = false
}
