package amqprpc

import (
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

// slogAttrsForTable returns a slice of slog.Attr from an amqp.Table. This will
// NOT filter out sensitive information. That should be done in a custom
// slog.Handler together with the users own application code.
func slogAttrsForTable(v amqp.Table) []slog.Attr {
	if len(v) == 0 {
		return nil
	}

	attrs := []slog.Attr{}

	for key, val := range v {
		if inner, ok := val.(amqp.Table); ok {
			attrs = append(attrs, slogGroupFor(
				key,
				slogAttrsForTable(inner),
			))

			continue
		}

		attrs = append(attrs, slog.Any(key, val))
	}

	return attrs
}

func slogAttrsForDelivery(v *amqp.Delivery) []slog.Attr {
	if v == nil {
		return nil
	}

	vals := []slog.Attr{
		slog.String("correlation_id", v.CorrelationId),
		slog.String("exchange", v.Exchange),
		slog.Bool("redelivered", v.Redelivered),
	}

	if v.RoutingKey != "" {
		vals = append(vals, slog.String("routing_key", v.RoutingKey))
	}

	if v.ContentType != "" {
		vals = append(vals, slog.String("content_type", v.ContentType))
	}

	if v.ContentEncoding != "" {
		vals = append(vals, slog.String("content_encoding", v.ContentEncoding))
	}

	if v.DeliveryMode != 0 {
		vals = append(vals, slog.Uint64("delivery_mode", uint64(v.DeliveryMode)))
	}

	if v.Priority != 0 {
		vals = append(vals, slog.Uint64("priority", uint64(v.Priority)))
	}

	if v.ReplyTo != "" {
		vals = append(vals, slog.String("reply_to", v.ReplyTo))
	}

	if v.Expiration != "" {
		vals = append(vals, slog.String("expiration", v.Expiration))
	}

	if v.MessageId != "" {
		vals = append(vals, slog.String("message_id", v.MessageId))
	}

	if !v.Timestamp.IsZero() {
		vals = append(vals, slog.Time("timestamp", v.Timestamp))
	}

	if v.Type != "" {
		vals = append(vals, slog.String("type", v.Type))
	}

	if v.UserId != "" {
		vals = append(vals, slog.String("user_id", v.UserId))
	}

	if v.AppId != "" {
		vals = append(vals, slog.String("app_id", v.AppId))
	}

	if v.ConsumerTag != "" {
		vals = append(vals, slog.String("consumer_tag", v.ConsumerTag))
	}

	if v.DeliveryTag != 0 {
		vals = append(vals, slog.Uint64("delivery_tag", v.DeliveryTag))
	}

	if v.Headers != nil {
		vals = append(vals, slogGroupFor("headers", slogAttrsForTable(v.Headers)))
	}

	return vals
}

func slogAttrsForPublishing(v *amqp.Publishing) []slog.Attr {
	if v == nil {
		return nil
	}

	vals := []slog.Attr{
		slog.String("correlation_id", v.CorrelationId),
	}

	if v.ContentType != "" {
		vals = append(vals, slog.String("content_type", v.ContentType))
	}

	if v.ContentEncoding != "" {
		vals = append(vals, slog.String("content_encoding", v.ContentEncoding))
	}

	if v.DeliveryMode != 0 {
		vals = append(vals, slog.Uint64("delivery_mode", uint64(v.DeliveryMode)))
	}

	if v.Priority != 0 {
		vals = append(vals, slog.Uint64("priority", uint64(v.Priority)))
	}

	if v.ReplyTo != "" {
		vals = append(vals, slog.String("reply_to", v.ReplyTo))
	}

	if v.Expiration != "" {
		vals = append(vals, slog.String("expiration", v.Expiration))
	}

	if v.MessageId != "" {
		vals = append(vals, slog.String("message_id", v.MessageId))
	}

	if !v.Timestamp.IsZero() {
		vals = append(vals, slog.Time("timestamp", v.Timestamp))
	}

	if v.Type != "" {
		vals = append(vals, slog.String("type", v.Type))
	}

	if v.UserId != "" {
		vals = append(vals, slog.String("user_id", v.UserId))
	}

	if v.AppId != "" {
		vals = append(vals, slog.String("app_id", v.AppId))
	}

	if v.Headers != nil {
		vals = append(vals, slogGroupFor("headers", slogAttrsForTable(v.Headers)))
	}

	return vals
}

func slogAttrsForRequest(v *Request) []slog.Attr {
	if v == nil {
		return nil
	}

	vals := []slog.Attr{
		slog.String("exchange", v.Exchange),
		slog.Bool("Reply", v.Reply),
		slogGroupFor("publishing", slogAttrsForPublishing(&v.Publishing)),
	}

	if v.RoutingKey != "" {
		vals = append(vals, slog.String("routing_key", v.RoutingKey))
	}

	if v.Mandatory {
		vals = append(vals, slog.Bool("mandatory", v.Mandatory))
	}

	if v.Context != nil {
		if deadline, ok := v.Context.Deadline(); ok {
			vals = append(vals, slog.Time("deadline", deadline))
		}
	}

	return vals
}

func slogAttrsForReturn(v *amqp.Return) []slog.Attr {
	if v == nil {
		return nil
	}

	vals := []slog.Attr{
		slog.String("correlation_id", v.CorrelationId),
		slog.Uint64("reply_code", uint64(v.ReplyCode)),
		slog.String("reply_text", v.ReplyText),
		slog.String("exchange", v.Exchange),
	}

	if v.RoutingKey != "" {
		vals = append(vals, slog.String("routing_key", v.RoutingKey))
	}

	if v.ContentType != "" {
		vals = append(vals, slog.String("content_type", v.ContentType))
	}

	if v.ContentEncoding != "" {
		vals = append(vals, slog.String("content_encoding", v.ContentEncoding))
	}

	if v.DeliveryMode != 0 {
		vals = append(vals, slog.Uint64("delivery_mode", uint64(v.DeliveryMode)))
	}

	if v.Priority != 0 {
		vals = append(vals, slog.Uint64("priority", uint64(v.Priority)))
	}

	if v.ReplyTo != "" {
		vals = append(vals, slog.String("reply_to", v.ReplyTo))
	}

	if v.Expiration != "" {
		vals = append(vals, slog.String("expiration", v.Expiration))
	}

	if v.MessageId != "" {
		vals = append(vals, slog.String("message_id", v.MessageId))
	}

	if !v.Timestamp.IsZero() {
		vals = append(vals, slog.Time("timestamp", v.Timestamp))
	}

	if v.Type != "" {
		vals = append(vals, slog.String("type", v.Type))
	}

	if v.UserId != "" {
		vals = append(vals, slog.String("user_id", v.UserId))
	}

	if v.AppId != "" {
		vals = append(vals, slog.String("app_id", v.AppId))
	}

	if v.Headers != nil {
		vals = append(vals, slogGroupFor("headers", slogAttrsForTable(v.Headers)))
	}

	return vals
}

func slogGroupFor(key string, attrs []slog.Attr) slog.Attr {
	return slog.Attr{
		Key:   key,
		Value: slog.GroupValue(attrs...),
	}
}
