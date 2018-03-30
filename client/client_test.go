package client

import "testing"

func TestNewClient(t *testing.T) {
	c := New()

	if c == nil {
		t.Error("Expected a client")
	}
}
