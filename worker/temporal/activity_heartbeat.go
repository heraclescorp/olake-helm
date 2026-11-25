package temporal

import (
	"context"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"go.temporal.io/sdk/activity"
)

func activityHeartbeat(ctx context.Context, req *types.ExecutionRequest) func() {
	if !activity.IsActivity(ctx) {
		return func() {}
	}
	req.HeartbeatFunc = activity.RecordHeartbeat
	command, jobID := req.Command, req.JobID
	activity.RecordHeartbeat(ctx, "executing", command, jobID)
	ticker := time.NewTicker(5 * time.Second)
	return runActivityHeartbeats(ctx, ticker.C, ticker.Stop, func() {
		activity.RecordHeartbeat(ctx, "executing", command, jobID)
	})
}

func runActivityHeartbeats(ctx context.Context, ticks <-chan time.Time, stopTicker, heartbeat func()) func() {
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticks:
				if ctx.Err() == nil {
					heartbeat()
				}
			}
		}
	}()
	return func() {
		stopTicker()
		close(stop)
		<-done
	}
}
