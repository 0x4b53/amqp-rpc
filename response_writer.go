package amqprpc

import "github.com/streadway/amqp"

/*
ResponseWriter is used by a handler to construct an RPC response.
The ResponseWriter may NOT be used after the handler has returned.

Because the ResponseWriter implements io.Writer you can for example use it to
write json:

	encoder := json.NewEncoder(responseWriter)
	encoder.Encode(dataObject)
*/
type ResponseWriter struct {
	publishing *amqp.Publishing
	mandatory  bool
	immediate  bool
}

// NewResponseWriter will create a new response writer with given amqp.Publishing.
func NewResponseWriter(p *amqp.Publishing) *ResponseWriter {
	return &ResponseWriter{
		publishing: p,
	}
}

// Write will write the response Body of the amqp.Publishing.
// It is safe to call Write multiple times.
func (rw *ResponseWriter) Write(p []byte) (int, error) {
	rw.publishing.Body = append(rw.publishing.Body, p...)
	return len(p), nil
}

// WriteHeader will write a header for the specified key.
func (rw *ResponseWriter) WriteHeader(header string, value interface{}) {
	if rw.publishing.Headers == nil {
		rw.publishing.Headers = map[string]interface{}{}
	}

	rw.publishing.Headers[header] = value
}

// Publishing returns the internal amqp.Publishing that are used for the
// response, useful for modification.
func (rw *ResponseWriter) Publishing() *amqp.Publishing {
	return rw.publishing
}

// Mandatory sets the mandatory flag on the later amqp.Publish.
func (rw *ResponseWriter) Mandatory(m bool) {
	rw.mandatory = m
}

// Immediate sets the immediate flag on the later amqp.Publish.
func (rw *ResponseWriter) Immediate(i bool) {
	rw.immediate = i
}
