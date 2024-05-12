package amqprpc

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func Benchmark(b *testing.B) {
	s := NewServer(testURL)
	queueName := uuid.New().String()
	s.Bind(DirectBinding(queueName, func(_ context.Context, _ *ResponseWriter, _ amqp.Delivery) {}))

	go s.ListenAndServe()
	time.Sleep(1 * time.Second)

	confirmingClient := NewClient(testURL).
		WithTimeout(3 * time.Minute).
		WithErrorLogger(log.Printf)

	defer confirmingClient.Stop()

	fastClient := NewClient(testURL).
		WithErrorLogger(log.Printf).
		WithTimeout(3 * time.Minute).
		WithPublishSettings(PublishSettings{
			Mandatory:   true,
			Immediate:   false,
			ConfirmMode: false,
		})

	defer fastClient.Stop()

	// Send a request to ensure the client have started.
	_, err := confirmingClient.Send(NewRequest().WithRoutingKey(queueName))
	if err != nil {
		b.Fatal("client/server not working")
	}

	// Send a request to ensure the client have started.
	_, err = fastClient.Send(NewRequest().WithRoutingKey(queueName))
	if err != nil {
		b.Fatal("client/server not working")
	}

	benchmarks := []struct {
		name         string
		withResponse bool
		returned     bool
		client       *Client
	}{
		{
			name:         "WithResponse-NoConfirmMode",
			withResponse: true,
			client:       fastClient,
		},
		{
			name:         "WithResponse-ConfirmMode",
			withResponse: true,
			client:       confirmingClient,
		},
		{
			name:         "WithResponse-Returned",
			withResponse: false,
			returned:     true,
			client:       confirmingClient,
		},
		{
			name:         "NoResponse-NoConfirmMode",
			withResponse: false,
			client:       fastClient,
		},
		{
			name:         "NoResponse-ConfirmMode",
			withResponse: false,
			client:       confirmingClient,
		},
		{
			name:         "NoResponse-Returned",
			withResponse: false,
			returned:     true,
			client:       confirmingClient,
		},
	}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf(bm.name), func(b *testing.B) {
			routingKey := queueName
			if bm.returned {
				routingKey = "does-not-exist"
			}

			time.Sleep(2 * time.Second) // Let the amqp-server calm down between the tests.
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					request := NewRequest().
						WithRoutingKey(routingKey).
						WithResponse(bm.withResponse)

					_, err := bm.client.Send(request)

					if bm.returned {
						if err == nil {
							b.Fatal("Expected err to be non nil")
						}
					} else {
						if err != nil {
							b.Fatal(err.Error())
						}
					}
				}
			})
		})
	}
}
