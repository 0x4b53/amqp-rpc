package amqprpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDialer(t *testing.T) {
	assert := assert.New(t)

	conn, err := DefaultDialer(DialConf{
		DialTimeout: 10 * time.Second,
		Deadline:    10 * time.Second,
	})("tcp", "gone.local")
	assert.Nil(conn, "no connection for bad host")
	assert.NotNil(err, "errors occurred")

	conn, err = DefaultDialer(DialConf{
		DialTimeout: 10 * time.Second,
		Deadline:    10 * time.Second,
	})("tcp", "localhost:5672")
	assert.Nil(err, "no error for correct hos")
	assert.NotNil(conn, "connection exist")
}
