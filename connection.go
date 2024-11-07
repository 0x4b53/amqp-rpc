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
