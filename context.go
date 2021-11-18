package amqprpc

import "context"

type ctxKey int

const (
	queueNameKey ctxKey = iota
	shutdownChanKey
)

// ContextWithQueueName adds the given queueName to the provided context.
func ContextWithQueueName(ctx context.Context, queueName string) context.Context {
	return context.WithValue(ctx, queueNameKey, queueName)
}

// QueueNameFromContext returns the queue name for the current request.
func QueueNameFromContext(ctx context.Context) (string, bool) {
	queueName, ok := ctx.Value(queueNameKey).(string)
	return queueName, ok
}

// ContextWithShutdownChan adds a shutdown chan to the given context.
func ContextWithShutdownChan(ctx context.Context, ch chan struct{}) context.Context {
	return context.WithValue(ctx, shutdownChanKey, ch)
}

// ShutdownChanFromContext returns the shutdown chan.
func ShutdownChanFromContext(ctx context.Context) (chan struct{}, bool) {
	ch, ok := ctx.Value(shutdownChanKey).(chan struct{})
	return ch, ok
}
