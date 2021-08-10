package amqprpc

import (
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
)

func TestPublisher(t *testing.T) {
	publ := NewPublisher(testURL)

	publ.Start()

	publ.Publish(PublishData{
		Exchange: ExchangeDefaultDirect,
		Key:      "foobar",
		Msg:      amqp.Publishing{},
	})

	publ.Publish(PublishData{
		Exchange: ExchangeDefaultDirect,
		Key:      "queue",
		Msg:      amqp.Publishing{},
	})

	err := publ.AwaitConfirms(2 * time.Second)
	require.NoError(t, err)

	var gotErr error
	var gotConf bool

	publ.Publish(PublishData{
		Exchange:  ExchangeDefaultDirect,
		Key:       "foobar",
		Mandatory: true,
		OnConfirm: func(err error) {
			gotConf = true
			gotErr = err
		},
	})

	err = publ.AwaitConfirms(2 * time.Second)
	require.NoError(t, err)

	require.True(t, gotConf)
	require.Error(t, gotErr)
}

func TestSomething(t *testing.T) {
}
