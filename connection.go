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

// OnConnectedFunc can be registered with [Server.OnConnected] and
// [Client.OnConnected]. This is used when you want to do more setup on the
// connections and/or channels from amqp, for example setting Qos,
// NotifyPublish etc.
type OnConnectedFunc func(inputConn, outputConn *amqp.Connection, inputChannel, outputChannel *amqp.Channel)

func monitorAndWait(
	restartChan,
	stopChan chan struct{},
	inputConnClose,
	outputConnClose,
	inputChClose,
	outputChClose chan *amqp.Error,
) (bool, error) {
	select {
	case <-restartChan:
		return true, nil

	case <-stopChan:
		return false, nil

	case err, ok := <-inputConnClose:
		if !ok {
			return false, ErrUnexpectedConnClosed
		}

		return false, err

	case err, ok := <-outputConnClose:
		if !ok {
			return false, ErrUnexpectedConnClosed
		}

		return false, err

	case err, ok := <-inputChClose:
		if !ok {
			return false, ErrUnexpectedConnClosed
		}

		return false, err

	case err, ok := <-outputChClose:
		if !ok {
			return false, ErrUnexpectedConnClosed
		}

		return false, err
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
