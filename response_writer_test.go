package amqprpc

import (
	"fmt"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestResponseWriter(t *testing.T) {
	rw := &ResponseWriter{
		Publishing: &amqp.Publishing{},
	}

	fmt.Fprint(rw, "Foo")
	assert.Equal(t, []byte("Foo"), rw.Publishing.Body, "writing to response writer is reflected in the body")

	fmt.Fprint(rw, "Bar")
	assert.Equal(t, []byte("FooBar"), rw.Publishing.Body, "writing to response writer multiple times is reflected in the body")

	rw.WriteHeader("some-header", "writing")
	assert.Equal(t, "writing", rw.Publishing.Headers["some-header"], "writing headers will set the headers of the publishing")

	rw.WriteHeader("some-header", "writing-again")
	assert.Equal(t, "writing-again", rw.Publishing.Headers["some-header"], "overwriting headers will set the headers of the publishing")

	rw.WriteHeader("some-header", 1)
	assert.Equal(t, 1, rw.Publishing.Headers["some-header"], "writing other types than s t rings to header works")
}
