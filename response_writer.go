package amqprpc

import amqp "github.com/rabbitmq/amqp091-go"

/*
ResponseWriter is used by a handler to construct an RPC response.
The ResponseWriter may NOT be used after the handler has returned.

Because the ResponseWriter implements io.Writer you can for example use it to
write json:

	encoder := json.NewEncoder(responseWriter)
	encoder.Encode(dataObject)
*/
type ResponseWriter struct {
	Publishing *amqp.Publishing
	Mandatory  bool
	Immediate  bool
}

// NewResponseWriter will create a new response writer with given amqp.Publishing.
func NewResponseWriter(p *amqp.Publishing) *ResponseWriter {
	return &ResponseWriter{
		Publishing: p,
	}
}

// Write will write the response Body of the amqp.Publishing.
// It is safe to call Write multiple times.
func (rw *ResponseWriter) Write(p []byte) (int, error) {
	rw.Publishing.Body = append(rw.Publishing.Body, p...)
	return len(p), nil
}

// WriteHeader will write a header for the specified key.
func (rw *ResponseWriter) WriteHeader(header string, value interface{}) {
	if rw.Publishing.Headers == nil {
		rw.Publishing.Headers = map[string]interface{}{}
	}

	rw.Publishing.Headers[header] = value
}
