package temporal

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestActivityHeartbeatsCoverBlockedPreparationAndStopBeforeReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticks := make(chan time.Time)
	heartbeats := make(chan struct{})
	stopped := 0
	stop := runActivityHeartbeats(ctx, ticks, func() { stopped++ }, func() { heartbeats <- struct{}{} })
	for range 7 {
		ticks <- time.Time{}
		<-heartbeats
	}
	stop()
	require.Equal(t, 1, stopped)
	select {
	case ticks <- time.Time{}:
		t.Fatal("heartbeat task outlived activity return")
	default:
	}
}

func TestActivityHeartbeatCancellationStopsBlockedPreparationLiveness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time)
	heartbeats := make(chan struct{}, 1)
	stop := runActivityHeartbeats(ctx, ticks, func() {}, func() { heartbeats <- struct{}{} })
	ticks <- time.Time{}
	<-heartbeats
	cancel()
	stop()
	select {
	case <-heartbeats:
		t.Fatal("heartbeat emitted after cancellation was acknowledged")
	default:
	}
}
