package logger

import (
	"log"
	"testing"

	. "gopkg.in/go-playground/assert.v1"
)

type MockLogger struct {
	called bool
	level  string
}

func (m *MockLogger) Print(v ...interface{}) {
	m.called = true
}

func (m *MockLogger) Printf(format string, v ...interface{}) {
	m.called = true
}

func (m *MockLogger) reset() {
	m.called = false
}

func TestLogger(t *testing.T) {
	l, ok := infoLogger.(*log.Logger)
	Equal(t, ok, true)
	NotEqual(t, l, nil)

	l, ok = warnLogger.(*log.Logger)
	Equal(t, ok, true)
	NotEqual(t, l, nil)

	mockLog := MockLogger{level: "info"}

	SetInfoLogger(&mockLog)

	Info("Test info")
	Equal(t, mockLog.called, true)
	mockLog.reset()

	Infof("Test infof with %d %s values", 2, "random")
	Equal(t, mockLog.called, true)
	mockLog.reset()

	mockLog.level = "warn"
	SetWarnLogger(&mockLog)

	Warn("Test warn")
	Equal(t, mockLog.called, true)
	mockLog.reset()

	Warnf("Test warnf with %d %s values", 2, "random")
	Equal(t, mockLog.called, true)
	mockLog.reset()
}
