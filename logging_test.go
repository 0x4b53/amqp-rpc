package amqprpc

import (
	"io"
	"io/ioutil"
	"log"
	"testing"
	"time"

	. "gopkg.in/go-playground/assert.v1"
)

func TestServerLogging(t *testing.T) {
	reader, writer := io.Pipe()

	go func() {
		logger := log.New(writer, "TEST", log.LstdFlags)

		s := NewServer(serverTestURL)
		s.WithDebugLogger(logger.Printf)
		s.WithErrorLogger(logger.Printf)

		stop := startAndWait(s)
		stop()

		writer.Close()
	}()

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	NotEqual(t, string(buf), "")
	MatchRegex(t, string(buf), "^TEST")
}

func TestClientLogging(t *testing.T) {
	reader, writer := io.Pipe()

	go func() {
		logger := log.New(writer, "TEST", log.LstdFlags)

		c := NewClient("amqp://guest:guest@localhost:5672/")
		c.WithDebugLogger(logger.Printf)
		c.WithErrorLogger(logger.Printf)

		c.Send(NewRequest().WithRoutingKey("foobar").WithTimeout(time.Millisecond))
		c.Stop()

		writer.Close()
	}()

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	NotEqual(t, string(buf), "")
	MatchRegex(t, string(buf), "^TEST")
}
