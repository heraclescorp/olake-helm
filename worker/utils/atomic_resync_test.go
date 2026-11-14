package utils

import (
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/stretchr/testify/require"
)

func TestResyncSelectorUsesStableWorkflowIdentity(t *testing.T) {
	base := []string{"resync", constants.ResyncNamespaceFlag, "public", constants.ResyncTableFlag, "target", constants.ResyncIDFlag, "operation"}
	for _, mode := range []string{"valid", "blank id", "different id", "missing namespace", "duplicate target", "clear command", "inline flags"} {
		t.Run(mode, func(t *testing.T) {
			req := &types.ExecutionRequest{Command: types.Resync, WorkflowID: "operation", Args: append([]string(nil), base...)}
			switch mode {
			case "blank id":
				req.Args[6] = " "
			case "different id":
				req.Args[6] = "other"
			case "missing namespace":
				req.Args = append(req.Args[:1], req.Args[3:]...)
			case "duplicate target":
				req.Args = append(req.Args, constants.ResyncTableFlag, "other")
			case "clear command":
				req.Command = types.ClearDestination
			case "inline flags":
				req.Args = []string{"resync", "--resync-namespace=public", "--resync-table=target", "--resync-id=operation"}
			}
			target, err := ResyncTargetFromRequest(req)
			if mode == "valid" || mode == "inline flags" {
				require.NoError(t, err)
				require.Equal(t, ResyncTarget{Namespace: "public", Table: "target", ID: "operation"}, target)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestResyncIsAsyncAndRestoresOnlyOrdinarySyncInputs(t *testing.T) {
	req := &types.ExecutionRequest{Command: types.Resync, WorkflowID: "operation", Args: []string{"resync", constants.AvailableStreamsFlag, "/mnt/config/available_streams.json", constants.ResyncIDFlag, "operation"}, Configs: []types.JobConfig{{Name: "state.json", Data: `{"candidate":true}`}}, TempPath: "must-not-restore"}
	require.True(t, IsAsyncCommand(req.Command))
	require.Equal(t, GetWorkflowDirectory(types.Sync, req.WorkflowID), GetWorkflowDirectory(types.Resync, req.WorkflowID))
	RevertUpdatesInSchedule(req)
	require.Equal(t, types.Sync, req.Command)
	require.Empty(t, req.Configs)
	require.Empty(t, req.TempPath)
	require.Equal(t, "sync", req.Args[0])
	require.Contains(t, req.Args, constants.StateFlag)
	require.Contains(t, req.Args, constants.AvailableStreamsFlag)
	require.Contains(t, req.Args, constants.SelectedStreamsFlag)
	require.NotContains(t, req.Args, constants.ResyncIDFlag)
}
