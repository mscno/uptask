package e2e

import (
	"fmt"
	"github.com/mscno/uptask/testutl"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSuite(t *testing.T) {
	port := testutl.GetPort()
	tunnelUrl, cmd, err := testutl.StartLocalTunnel(port)
	require.NoError(t, err)
	t.Cleanup(func() {
		fmt.Println("Killing local tunnel")
		cmd.Process.Kill()
	})

	t.Run("SingleEvent", func(t *testing.T) { SingleEventTest(t, port, tunnelUrl) })

	t.Run("SingleTaskTest", func(t *testing.T) { SingleTaskTest(t, port, tunnelUrl) })

	t.Run("RoomBookFlow", func(t *testing.T) { RoomBookFlow(t, port, tunnelUrl) })

	t.Run("SnoozeTask", func(t *testing.T) { SnoozeTaskTest(t, port, tunnelUrl) })

	t.Run("SnoozeEvent", func(t *testing.T) { SnoozeEventTest(t, port, tunnelUrl) })

	t.Run("RetryTask", func(t *testing.T) { RetryTaskTest(t, port, tunnelUrl) })

	t.Run("RetryEvent", func(t *testing.T) { RetryEventTest(t, port, tunnelUrl) })

	t.Run("DlqTask", func(t *testing.T) { DlqTaskTest(t, port, tunnelUrl) })
}
