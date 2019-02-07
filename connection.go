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

// OnStartedFunc can be registered at Server.OnStarted(f) and
// Client.OnStarted(f). This is used when you want to do more setup on the
// connections and/or channels from amqp, for example setting Qos,
// NotifyPublish etc.
type OnStartedFunc func(inputConn, outputConn *amqp.Connection, inputChannel, outputChannel *amqp.Channel)

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

func monitorAndWait(stopChan chan struct{}, amqpErrs ...chan *amqp.Error) error {
	result := make(chan error, len(amqpErrs))

	// Setup monitoring for connections and channels, can be several connections and several channels.
	// The first one closed will yield the error.
	for _, errCh := range amqpErrs {
		go func(c chan *amqp.Error) {
			err, ok := <-c
			if !ok {
				result <- ErrUnexpectedConnClosed
				return
			}
			result <- err
		}(errCh)
	}

	select {
	case err := <-result:
		return err
	case <-stopChan:
		return nil
	}
}

func createConnections(url string, config amqp.Config) (*amqp.Connection, *amqp.Connection, error) {
	var (
		conn1 *amqp.Connection
		conn2 *amqp.Connection
		err   error
	)
	conn1, err = amqp.DialConfig(url, config)
	if err != nil {
		return nil, nil, err
	}

	conn2, err = amqp.DialConfig(url, config)
	if err != nil {
		return nil, nil, err
	}

	return conn1, conn2, nil
}

func createChannels(inputConn, outputConn *amqp.Connection) (*amqp.Channel, *amqp.Channel, error) {
	inputCh, err := inputConn.Channel()
	if err != nil {
		return nil, nil, err
	}

	outputCh, err := outputConn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return inputCh, outputCh, nil
}
