package temporal

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
)

func resyncWorkflowArgs(startSync bool) map[string]interface{} {
	return map[string]interface{}{
		"command":                string(types.ClearDestination),
		"project_id":             "123",
		"job_id":                 42,
		"temp_path":              "table-resync/state.json",
		"start_sync_after_clear": startSync,
	}
}

type resyncServiceClient struct {
	workflowservice.WorkflowServiceClient
	patch func(context.Context, *workflowservice.PatchScheduleRequest) (*workflowservice.PatchScheduleResponse, error)
}

func (c *resyncServiceClient) PatchSchedule(ctx context.Context, req *workflowservice.PatchScheduleRequest, _ ...grpc.CallOption) (*workflowservice.PatchScheduleResponse, error) {
	return c.patch(ctx, req)
}

type resyncTemporalClient struct {
	client.Client
	service   workflowservice.WorkflowServiceClient
	schedules client.ScheduleClient
}

func (c *resyncTemporalClient) WorkflowService() workflowservice.WorkflowServiceClient {
	return c.service
}

func (c *resyncTemporalClient) ScheduleClient() client.ScheduleClient {
	return c.schedules
}

func TestCancelledClearFollowupDoesNotSendPatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	activities := &Activity{}
	err := activities.TriggerSyncAfterClearActivity(ctx, &types.ExecutionRequest{StartSyncAfterClear: true})
	require.ErrorIs(t, err, context.Canceled)
}
