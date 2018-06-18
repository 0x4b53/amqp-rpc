package server

// Middlewares builds a middlware chain recursively.
func Middlewares(next HandlerFunc, m ...func(HandlerFunc) HandlerFunc) HandlerFunc {
	// if our chain is done, use the original handlerfunc
	if len(m) == 0 {
		return next
	}

	// otherwise nest the handlerfuncs
	return m[0](Middlewares(next, m[1:cap(m)]...))
}
