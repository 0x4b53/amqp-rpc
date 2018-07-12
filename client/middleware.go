package client

// MiddlewarePreFunc represents the function type that's being executed before
// the Send() method publishes the message.
type MiddlewarePreFunc func(next PreSendFunc) PreSendFunc

// MiddlewarePostFunc represents the function type of functions being executed
// after the Send() method has published a message. Since the PostSendFunc
// takes an amqp.Delivery as first argument these methods are not executed if an
// error occurs or if the request was created without wanting a reply.
type MiddlewarePostFunc func(next PostSendFunc) PostSendFunc

// MiddlewarePreChain will chain all middlewares passed and return a
// PreSendFunc.
func MiddlewarePreChain(next PreSendFunc, m ...MiddlewarePreFunc) PreSendFunc {
	if len(m) == 0 {
		return next
	}

	return m[0](MiddlewarePreChain(next, m[1:cap(m)]...))
}

// MiddlewarePostChain will chain all middlewares passed and return a
// PostSendFunc.
func MiddlewarePostChain(next PostSendFunc, m ...MiddlewarePostFunc) PostSendFunc {
	if len(m) == 0 {
		return next
	}

	return m[0](MiddlewarePostChain(next, m[1:cap(m)]...))
}
