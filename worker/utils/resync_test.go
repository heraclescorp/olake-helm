package utils

import (
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/stretchr/testify/require"
)

func TestTableResyncRestoresFullSavedCatalog(t *testing.T) {
	for _, split := range []bool{false, true} {
		name := "legacy catalog"
		if split {
			name = "split catalog"
		}
		t.Run(name, func(t *testing.T) {
			req := &types.ExecutionRequest{
				Command:  types.Resync,
				TempPath: "table-resync/state.json",
				Args:     []string{"resync", "--state", "/mnt/config/state.json", "--destination", "/mnt/config/destination.json"},
				Configs: []types.JobConfig{
					{Name: constants.StreamsFile, Data: `{"streams":[{"name":"reset"}]}`},
					{Name: constants.AvailableStreamsFile, Data: `{"streams":[{"name":"reset"}]}`},
					{Name: constants.SelectedStreamsFile, Data: `{"streams":[{"name":"reset"}]}`},
				},
			}
			job := types.JobData{
				Source:      `{"host":"saved-source"}`,
				Destination: `{"host":"saved-destination"}`,
				State:       `{"global":{"lsn":"12345"},"streams":[{"name":"other","cursor":"6789"}]}`,
				Version:     "v0.8.2",
			}
			fullCatalog := `{"streams":[{"name":"reset"},{"name":"other"}]}`
			if split {
				job.AvailableStreams = fullCatalog
				job.SelectedStreams = fullCatalog
				req.Args = SetCatalogArgs(req.Args, true)
			} else {
				job.Streams = fullCatalog
				req.Args = SetCatalogArgs(req.Args, false)
			}

			RevertUpdatesInSchedule(req)
			require.Equal(t, types.Sync, req.Command)
			require.Empty(t, req.TempPath)
			require.Empty(t, req.Configs)
			require.Equal(t, "sync", req.Args[0])

			UpdateConfigWithJobDetails(job, req)
			require.Equal(t, job.Version, req.Version)
			configs := make(map[string]string)
			for _, config := range req.Configs {
				configs[config.Name] = config.Data
			}
			require.Equal(t, job.Source, configs["source.json"])
			require.Equal(t, job.Destination, configs["destination.json"])
			require.Equal(t, job.State, configs["state.json"])
			if split {
				require.Equal(t, fullCatalog, configs[constants.AvailableStreamsFile])
				require.Equal(t, fullCatalog, configs[constants.SelectedStreamsFile])
				require.Contains(t, req.Args, constants.AvailableStreamsFlag)
				require.Contains(t, req.Args, constants.SelectedStreamsFlag)
				require.NotContains(t, req.Args, constants.CatalogFlag)
			} else {
				require.Equal(t, fullCatalog, configs[constants.StreamsFile])
				require.Contains(t, req.Args, constants.CatalogFlag)
				require.NotContains(t, req.Args, constants.AvailableStreamsFlag)
				require.NotContains(t, req.Args, constants.SelectedStreamsFlag)
			}
		})
	}
}

func TestOrdinaryClearRequestKeepsExistingBehavior(t *testing.T) {
	req := &types.ExecutionRequest{Command: types.ClearDestination, TempPath: "ordinary-clear/state.json", Configs: []types.JobConfig{{Name: "state.json", Data: `{"keep":true}`}}}
	RevertUpdatesInSchedule(req)
	require.Equal(t, types.Sync, req.Command)
	require.Equal(t, "ordinary-clear/state.json", req.TempPath)
	require.Equal(t, []types.JobConfig{{Name: "state.json", Data: `{"keep":true}`}}, req.Configs)
}
