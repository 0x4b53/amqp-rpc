package amqprpc

import (
	"errors"
	"fmt"
	"maps"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrUnexpectedConnClosed is returned by ListenAndServe() if the server
// shuts down without calling Stop() and if AMQP does not give an error
// when said shutdown happens.
var ErrUnexpectedConnClosed = errors.New("unexpected connection close without specific error")

// OnStartedFunc can be registered at Server.OnStarted(f) and
// Client.OnStarted(f). This is used when you want to do more setup on the
// connections and/or channels from amqp, for example setting Qos,
// NotifyPublish etc.
type OnStartedFunc func(inputConn, outputConn *amqp.Connection, inputChannel, outputChannel *amqp.Channel)

// ExchangeDeclareSettings is the settings that will be used when a handler
// is mapped to a fanout exchange and an exchange is declared.
type ExchangeDeclareSettings struct {
	// Name is the name of the exchange.
	Name string

	// Type is the exchange type.
	Type string

	// Durable sets the durable flag. Durable exchanges survives server restart.
	Durable bool

	// AutoDelete sets the auto-delete flag, this ensures the exchange is
	// deleted when it isn't bound to any more.
	AutoDelete bool

	// Args sets the arguments table used.
	Args amqp.Table
}

// PublishSettings is the settings that will be used when a message is about
// to be published to the message bus. These settings are only used by the
// Client and never by the Server. For the server, Mandatory or Immediate
// can be set on the ResponseWriter instead.
type PublishSettings struct {
	// Mandatory sets the mandatory flag. When this is true a Publish call will
	// be returned if it's not routable by the exchange.
	Mandatory bool

	// Immediate sets the immediate flag. When this is true a Publish call will
	// be returned if a consumer isn't directly available.
	Immediate bool

	// ConfirmMode puts the channel that messages are published over in
	// confirm mode. This makes sending requests more reliable at the cost
	// of some performance. Each publishing must be confirmed by the server.
	// See https://www.rabbitmq.com/confirms.html#publisher-confirms
	ConfirmMode bool
}

func monitorAndWait(restartChan, stopChan chan struct{}, amqpErrs ...chan *amqp.Error) (bool, error) {
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
		return true, err
	case <-restartChan:
		return true, nil
	case <-stopChan:
		return false, nil
	}
}

func createConnections(url, name string, config amqp.Config) (consumerConn, publisherConn *amqp.Connection, err error) {
	if config.Properties == nil {
		config.Properties = amqp.Table{}
	}

	consumerConnConfig := config
	publisherConnConfig := config

	if _, ok := config.Properties["connection_name"]; !ok {
		consumerConnConfig.Properties = maps.Clone(config.Properties)
		publisherConnConfig.Properties = maps.Clone(config.Properties)

		consumerConnConfig.Properties["connection_name"] = fmt.Sprintf("%s-consumer", name)
		publisherConnConfig.Properties["connection_name"] = fmt.Sprintf("%s-publisher", name)
	}

	consumerConn, err = amqp.DialConfig(url, consumerConnConfig)
	if err != nil {
		return nil, nil, err
	}

	publisherConn, err = amqp.DialConfig(url, publisherConnConfig)
	if err != nil {
		return nil, nil, err
	}

	return consumerConn, publisherConn, nil
}

func createChannels(inputConn, outputConn *amqp.Connection) (inputCh, outputCh *amqp.Channel, err error) {
	inputCh, err = inputConn.Channel()
	if err != nil {
		return nil, nil, err
	}

	outputCh, err = outputConn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return inputCh, outputCh, nil
}
