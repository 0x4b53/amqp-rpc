package testhelpers

import (
	"testing"
	"time"

	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

var url = "amqp://guest:guest@localhost:5672/"

func TestTestDialer(t *testing.T) {
	dialer, connections := TestDialer(t)
	amqpConn, err := amqp.DialConfig(url, amqp.Config{
		Dial: dialer,
	})
	Equal(t, err, nil)
	NotEqual(t, amqpConn, nil)

	select {
	case connection, ok := <-connections:
		Equal(t, ok, true)
		NotEqual(t, connection, nil)
	case <-time.After(100 * time.Millisecond):
		t.Fail()
	}
}
