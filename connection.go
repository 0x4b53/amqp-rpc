package amqprpc

import (
	"errors"

	"github.com/streadway/amqp"
)

var (
	// ErrUnexpectedConnClosed is returned by ListenAndServe() if the server
	// shuts down without calling Stop() and if AMQP does not give an error
	// when said shutdown happens.
	ErrUnexpectedConnClosed = errors.New("unexpected connection close without specific error")

	// ErrTimeout is an error returned when a client request does not
	// receive a response within the client timeout duration.
	ErrTimeout = errors.New("request timed out")
)

// ExchangeDeclareSettings is the settings that will be used when a handler
// is mapped to a fanout exchange and an exchange is declared.
type ExchangeDeclareSettings struct {
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

// QueueDeclareSettings is the settings that will be used when the response
// any kind of queue is declared. Se documentation for amqp.QueueDeclare
// for more information about these settings.
type QueueDeclareSettings struct {
	Durable          bool
	DeleteWhenUnused bool
	Exclusive        bool
	NoWait           bool
	Args             amqp.Table
}

// ConsumeSettings is the settings that will be used when the consumption
// on a specified queue is started.
type ConsumeSettings struct {
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

// PublishSettings is the settings that will be used when a message is about
// to be published to the message bus.
type PublishSettings struct {
	Mandatory bool
	Immediate bool
}

func monitorChannels(stopChan chan struct{}, connectionChannels []chan *amqp.Error) error {
	result := make(chan error)

	// Setup monitoring for connection channels, can be several connections.
	// The first one closed will yield the error.
	for _, c := range connectionChannels {
		go func(c chan *amqp.Error) {
			err, ok := <-c
			if !ok {
				result <- ErrUnexpectedConnClosed
			}

			result <- err
		}(c)
	}

	// Setup monitoring for application channel
	go func() {
		<-stopChan
		result <- nil
	}()

	return <-result
}
