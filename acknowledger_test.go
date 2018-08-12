package amqprpc

import (
	"testing"

	. "gopkg.in/go-playground/assert.v1"
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
	ma := mockAcknowledger{}
	aac := ackAwareChannel{ch: &ma, handled: false}

	// 1
	aac.Ack(1, false)

	Equal(t, aac.handled, true)
	Equal(t, ma.ack, 1)

	aac.handled = false

	// 2
	aac.Nack(2, false, false)

	Equal(t, aac.handled, true)
	Equal(t, ma.nack, 1)

	aac.handled = false

	// 3
	aac.Reject(3, false)

	Equal(t, aac.handled, true)
	Equal(t, ma.reject, 1)

	aac.handled = false
}
