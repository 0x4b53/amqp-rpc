package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"

	amqprpc "github.com/0x4b53/amqp-rpc"
)

const testURL = "amqp://guest:guest@localhost:5672/"

func main() {
	queueName := "basic-bench"

	wg := sync.WaitGroup{}
	wg.Add(10)

	confirmingClient := amqprpc.NewClient(testURL).
		WithDebugLogger(func(format string, args ...interface{}) {}).
		WithErrorLogger(func(format string, args ...interface{}) {}).
		// WithTimeout(1 * time.Second).
		WithConfirmMode(true)

	defer confirmingClient.Stop()

	server := amqprpc.NewServer(testURL).
		WithAutoAck(false).
		WithQueueDeclareSettings(amqprpc.QueueDeclareSettings{
			Args: map[string]interface{}{
				"x-max-length": 100,
				"x-overflow":   "reject-publish",
			},
		})

	server.Bind(amqprpc.DirectBinding(queueName, func(ctx context.Context, rw *amqprpc.ResponseWriter, d amqp.Delivery) {
		fmt.Printf("Got message: %s\n", d.Body)
		d.Ack(false)
	}))

	go server.ListenAndServe()

	requests := make(chan int, 10000)

	go func() {
		for i := 0; i < 10000; i++ {
			requests <- i
		}
	}()

	for i := 0; i < 10; i++ {
		go func() {
			for req := range requests {
				var err error
				for {
					_, err = confirmingClient.Send(
						amqprpc.NewRequest().
							WithBody(fmt.Sprintf("%d", req)).
							WithResponse(false).
							WithRoutingKey(queueName),
					)
					if errors.Is(err, amqprpc.ErrRequestRejected) {
						time.Sleep(100 * time.Millisecond)
						continue
					}

					break
				}

				if err != nil {
					panic(err)
				}
			}

			wg.Done()
		}()
	}

	wg.Wait()
}
