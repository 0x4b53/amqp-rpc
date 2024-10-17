package amqprpc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestCreateConnections(t *testing.T) {
	hasConnections := func(t *testing.T, names ...string) bool {
		connectionsURL := fmt.Sprintf("%s/connections", serverAPITestURL)

		resp, err := http.Get(connectionsURL)
		require.NoError(t, err)

		result := []struct {
			UserProvidedName string `json:"user_provided_name"`
		}{}

		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)

		_ = resp.Body.Close()

		foundNames := []string{}

		for _, conn := range result {
			foundNames = append(foundNames, conn.UserProvidedName)
		}

		for _, wantName := range names {
			if !slices.Contains(foundNames, wantName) {
				return false
			}
		}

		return true
	}

	tests := []struct {
		name                  string
		config                amqp.Config
		wantConsumerConnName  string
		wantPublisherConnName string
	}{
		{
			name: "nil Properties sets connection names",
			config: amqp.Config{
				Properties: nil,
			},
			wantConsumerConnName:  "testingName-consumer",
			wantPublisherConnName: "testingName-publisher",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := amqp.Config{
				Properties: amqp.Table{},
				Dial:       amqp.DefaultDial(time.Second),
			}

			consumerConn, publisherConn, err := createConnections(testURL, "testingName", config)
			require.NoError(t, err)

			require.Eventually(t,
				func() bool { return hasConnections(t, tt.wantConsumerConnName, tt.wantPublisherConnName) },
				10*time.Second,
				100*time.Millisecond,
			)

			consumerConn.Close()
			publisherConn.Close()
		})
	}
}
