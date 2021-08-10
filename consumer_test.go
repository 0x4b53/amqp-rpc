package amqprpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
)

func TestConsumer(t *testing.T) {
	consumer := NewConsumerMultiplexer(testURL).
		WithDialConfig(
			amqp.Config{
				Properties: amqp.Table{
					"connection_name": testServerConnectionName,
				},
			},
		)

	ctx, stop := context.WithCancel(context.Background())

	t.Cleanup(stop)

	consumer.Start(ctx)

	messages1, err := consumer.Consume(ConsumerDefinition{
		QueueName:       uuid.New().String(),
		Tag:             "one",
		AutoAck:         false,
		RoutingKey:      "foobar",
		ExchangeName:    "amq.topic",
		ExchangeType:    "topic",
		AutoDeleteQueue: false,
		DurableExchange: true,
	})
	require.NoError(t, err)

	messages2, err := consumer.Consume(ConsumerDefinition{
		QueueName:       uuid.New().String(),
		Tag:             "two",
		AutoAck:         false,
		RoutingKey:      "foobar",
		ExchangeName:    "amq.topic",
		ExchangeType:    "topic",
		AutoDeleteQueue: false,
		DurableExchange: true,
	})
	require.NoError(t, err)
	require.NotNil(t, messages1)
	require.NotNil(t, messages2)

	conn, err := amqp.Dial(testURL)
	require.NoError(t, err)

	ch, err := conn.Channel()
	require.NoError(t, err)

	err = ch.Publish("amq.topic", "foobar", false, false, amqp.Publishing{})
	require.NoError(t, err)

	msg1 := <-messages1
	msg2 := <-messages2

	msg1.Ack(false)
	msg2.Ack(false)

	require.NotZero(t, msg1)
	require.NotZero(t, msg2)

	_, err = consumer.Consume(ConsumerDefinition{
		QueueName:       defaultTestQueue,
		Tag:             "tree",
		AutoAck:         true,
		RoutingKey:      "foobar",
		ExchangeName:    "amq.topic",
		ExchangeType:    "topic",
		AutoDeleteQueue: true,
		DurableExchange: false, // amqp.topic cannot be deleted.
	})
	require.Error(t, err)

	require.Equal(t, 2, len(consumer.consumers))

	fmt.Println("closing now")
	closeConnections(testServerConnectionName)

	err = ch.Publish("amq.topic", "foobar", false, false, amqp.Publishing{})
	require.NoError(t, err)

	fmt.Println("did publish it")

	msg1 = <-messages1
	msg2 = <-messages2

	require.NotZero(t, msg1)
	require.NotZero(t, msg2)

	msg1.Ack(false)
	msg2.Ack(false)
}
