package server

import (
	"fmt"
	"testing"

	"github.com/streadway/amqp"
	. "gopkg.in/go-playground/assert.v1"
)

func TestResponseWriter(t *testing.T) {
	rw := &ResponseWriter{
		publishing: &amqp.Publishing{},
	}

	// The default values are false.
	Equal(t, rw.immediate, false)
	Equal(t, rw.mandatory, false)

	// Setting true reflects internally.
	rw.Immediate(true)
	rw.Mandatory(true)
	Equal(t, rw.immediate, true)
	Equal(t, rw.mandatory, true)

	// Setting false reflects internally.
	rw.Immediate(false)
	rw.Mandatory(false)
	Equal(t, rw.immediate, false)
	Equal(t, rw.mandatory, false)

	// Writing to it reflects on the body.
	fmt.Fprint(rw, "Foo")
	Equal(t, rw.Publishing().Body, []byte("Foo"))

	// Writing multiple times is OK.
	fmt.Fprint(rw, "Bar")
	Equal(t, rw.Publishing().Body, []byte("FooBar"))
}
