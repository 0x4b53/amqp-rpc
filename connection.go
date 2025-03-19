package amqprpc

import (
	"errors"
	"fmt"
	"maps"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	// ErrUnexpectedConnClosed happens when the [Server] or [Client] gets a
	// connection closed without a specific error from amqp091. This usually
	// happens if the user calls Close() on a amqp connection or channel.
	ErrUnexpectedConnClosed = errors.New("unexpected connection close without specific error")

	// ErrConnectFailed can happen if the [Server] or [Client] fails to dial
	// the amqp server. It can also happen if a recently created connection
	// fails when creating channels.
	ErrConnectFailed = errors.New("failed to connect to AMQP")

	// ErrExchangeCreateFailed can happen if the [Server] fails to create the
	// exchanges given in [Server.WithExchanges].
	ErrExchangeCreateFailed = errors.New("failed to create exchanges")

	// ErrConsumerStartFailed can happen if the [Server] fails to start the
	// consumers for bindings created in [Server.Bind]. Or if the [Client]
	// fails to create it's reply-to consumer.
	ErrConsumerStartFailed = errors.New("failed to start consumers")
)

// OnConnectedFunc can be registered with [Server.OnConnected] and
// [Client.OnConnected]. This is used when you want to do more setup on the
// connections and/or channels from amqp, for example setting Qos,
// NotifyPublish etc.
type OnConnectedFunc func(inputConn, outputConn *amqp.Connection, inputChannel, outputChannel *amqp.Channel)

// OnErrorFunc can be registered with [Server.OnError] and [Client.OnError].
// This is used when you want to watch for any connection errors that occur.
type OnErrorFunc func(err error)

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
