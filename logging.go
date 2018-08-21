package amqprpc

/*
LogFunc is used for logging in amqp-rpc. It makes it possible to define your own logging.

Here is an example where the logger from the log package is used:

	debugLogger := log.New(os.Stdout, "DEBUG - ", log.LstdFlags)
	errorLogger := log.New(os.Stdout, "ERROR - ", log.LstdFlags)

	server := NewServer(url)
	server.WithErrorLogger(errorLogger.Printf)
	server.WithDebugLogger(debugLogger.Printf)

It can also be used with for example a Logrus logger:

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.Formatter = &logrus.JSONFormatter{}

	s.WithErrorLogger(logger.Warnf)
	s.WithDebugLogger(logger.Debugf)

	client := NewClient(url)
	client.WithErrorLogger(logger.Errorf)
	client.WithDebugLogger(logger.Debugf)

*/
type LogFunc func(format string, args ...interface{})
