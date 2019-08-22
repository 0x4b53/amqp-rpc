package amqprpc

import (
	"fmt"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestResponseWriter(t *testing.T) {
	assert := assert.New(t)

	rw := &ResponseWriter{
		Publishing: &amqp.Publishing{},
	}

	assert.Equal(false, rw.Immediate, "immediate starts false")
	assert.Equal(false, rw.Mandatory, "mandatory starts false")

	rw.Immediate = true
	rw.Mandatory = true

	assert.Equal(true, rw.Immediate, "immediate is changed to true")
	assert.Equal(true, rw.Mandatory, "mandatory is changed to true")

	rw.Immediate = false
	rw.Mandatory = false

	assert.Equal(false, rw.Immediate, "immediate are changed to false")
	assert.Equal(false, rw.Mandatory, "mandatory is changed to false")

	fmt.Fprint(rw, "Foo")
	assert.Equal([]byte("Foo"), rw.Publishing.Body, "writing to response writer is reflected in the body")

	fmt.Fprint(rw, "Bar")
	assert.Equal([]byte("FooBar"), rw.Publishing.Body, "writing to response writer multiple times is reflected in the body")

	rw.WriteHeader("some-header", "writing")
	assert.Equal("writing", rw.Publishing.Headers["some-header"], "writing headers will set the headers of the publishing")

	rw.WriteHeader("some-header", "writing-again")
	assert.Equal("writing-again", rw.Publishing.Headers["some-header"], "overwriting headers will set the headers of the publishing")

	rw.WriteHeader("some-header", 1)
	assert.Equal(1, rw.Publishing.Headers["some-header"], "writing other types than s t rings to header works")
}
