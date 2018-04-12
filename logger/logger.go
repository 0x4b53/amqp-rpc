package logger

import (
	"log"
	"os"
)

var (
	infoLogger Logger
	warnLogger Logger
)

// Logger is an interface which implements basic logger features.
type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}

func init() {
	infoLogger = log.New(os.Stdout, "", log.LstdFlags)
	warnLogger = log.New(os.Stdout, "", log.LstdFlags)
}

// SetInfoLogger will set the info level logger
func SetInfoLogger(l Logger) {
	infoLogger = l
}

// SetWarnLogger will set the warn level logger
func SetWarnLogger(l Logger) {
	warnLogger = l
}

// Info will lol
func Info(v ...interface{}) {
	infoLogger.Print(v...)
}

// Infof will lol
func Infof(format string, v ...interface{}) {
	infoLogger.Printf(format, v...)
}

// Warn will lol
func Warn(v ...interface{}) {
	warnLogger.Print(v...)
}

// Warnf will lol
func Warnf(format string, v ...interface{}) {
	warnLogger.Printf(format, v...)
}
