package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/stretchr/testify/require"
)

func TestOrdinaryRecoveryCatalogHashUsesExactNameLengthByteFraming(t *testing.T) {
	for _, split := range []bool{false, true} {
		files := map[string]string{}
		framed := ""
		if split {
			files[constants.AvailableStreamsFile] = `{"streams":[]}`
			files[constants.SelectedStreamsFile] = `{"selected_streams":{}}`
			framed = "available_streams.json\x0014\x00" + files[constants.AvailableStreamsFile] + "selected_streams.json\x0023\x00" + files[constants.SelectedStreamsFile]
		} else {
			files[constants.StreamsFile] = `{"streams":[]}`
			framed = "streams.json\x0014\x00" + files[constants.StreamsFile]
		}
		// Byte lengths here are deliberately literal to pin the cross-repo wire
		// protocol independently of the production hash implementation.
		digest := sha256.Sum256([]byte(framed))
		actual, err := OrdinaryCatalogSHA256(files)
		require.NoError(t, err)
		require.Equal(t, hex.EncodeToString(digest[:]), actual)
	}
	for _, files := range []map[string]string{
		{}, {constants.AvailableStreamsFile: `{}`}, {constants.SelectedStreamsFile: `{}`},
		{constants.StreamsFile: `{}`, constants.AvailableStreamsFile: `{}`, constants.SelectedStreamsFile: `{}`},
	} {
		_, err := OrdinaryCatalogSHA256(files)
		require.Error(t, err)
	}
	legacy, err := OrdinaryCatalogSHA256(map[string]string{constants.StreamsFile: `{"streams":[]}`})
	require.NoError(t, err)
	changed, err := OrdinaryCatalogSHA256(map[string]string{constants.StreamsFile: ` {"streams":[]} `})
	require.NoError(t, err)
	require.NotEqual(t, legacy, changed, "config digests bind exact original bytes, not normalized JSON")
}

func TestOrdinaryRecoveryFlagsCannotBeInjectedIntoFreshOrUnsupportedCommands(t *testing.T) {
	for _, command := range []types.Command{types.Sync, types.ClearDestination, types.Resync} {
		req := executionFenceFixture(t)
		req.Command = command
		req.Args = []string{string(command), "--ordinary-recovery-origin=attacker", "--ordinary-recovery-result", "attacker", "--execution-launch-id", "attacker"}
		SetExecutionArgs(req)
		joined := strings.Join(req.Args, " ")
		require.NotContains(t, joined, "attacker")
		require.NotContains(t, joined, "--ordinary-recovery-origin")
		require.NotContains(t, joined, "--ordinary-recovery-result")
	}
	req := executionFenceFixture(t)
	req.Command, req.RecoverOnly = types.Sync, true
	req.Args = []string{"sync"}
	SetExecutionArgs(req)
	SetExecutionArgs(req)
	joined := strings.Join(req.Args, " ")
	require.Equal(t, 1, strings.Count(joined, "--ordinary-recovery-origin"))
	require.Equal(t, 1, strings.Count(joined, "--ordinary-recovery-result"))
	require.Contains(t, joined, "--execution-launch-id "+req.LaunchID)
	require.NotContains(t, joined, "--resync-recover-only")
}

func TestOrdinaryRecoveryRejectsInvalidPostgresBoundaries(t *testing.T) {
	for _, value := range []string{"", "0", "0/", "/0", "0/100/200", "0/100000000", "-1/0", "0/xyz"} {
		_, err := ParseRecoveryLSN(value)
		require.Error(t, err, value)
	}
	value, err := ParseRecoveryLSN("1/00000001")
	require.NoError(t, err)
	require.Equal(t, uint64(1)<<32|1, value)
}

func TestSortPolicyEditsChangeRevisionAndRemainOutsideOrdinaryRecovery(t *testing.T) {
	const preset = `[{"column":"created_at","direction":"asc","null_order":"nulls-last"}]`
	const custom = `[{"column":"id","direction":"desc","null_order":"nulls-first"}]`
	const selection = `{"default_sort_order":` + preset + `,"selected_streams":{"public":[{"stream_name":"old","sort_order":` + custom + `}]}}`
	for _, split := range []bool{false, true} {
		for _, mutation := range []struct{ from, to string }{
			{`"default_sort_order":` + preset, `"default_sort_order":[]`},
			{`"default_sort_order":` + preset + `,`, ""},
			{`,"sort_order":` + custom, ""},
			{`"sort_order":` + custom, `"sort_order":[]`},
		} {
			job := types.JobData{Driver: "postgres", DestinationDriver: "iceberg", Source: `{"host":"source"}`, Destination: `{"type":"ICEBERG","writer":{"catalog_type":"rest"}}`, State: `{}`}
			files := map[string]string{"source.json": job.Source, "destination.json": job.Destination}
			catalog, filename := &job.Streams, constants.StreamsFile
			if split {
				job.AvailableStreams = `{"streams":[]}`
				files[constants.AvailableStreamsFile] = job.AvailableStreams
				catalog, filename = &job.SelectedStreams, constants.SelectedStreamsFile
				*catalog = selection
			} else {
				*catalog = strings.Replace(selection, `{`, `{"streams":[],`, 1)
			}
			files[filename] = *catalog
			stateSHA, err := StateSHA256(job.State)
			require.NoError(t, err)
			require.NoError(t, ValidateOrdinaryRecoveryJob(job, files, stateSHA))
			revision, err := job.CatalogRevision()
			require.NoError(t, err)
			originalHash, err := OrdinaryCatalogSHA256(files)
			require.NoError(t, err)
			*catalog = strings.Replace(*catalog, mutation.from, mutation.to, 1)
			changedRevision, err := job.CatalogRevision()
			require.NoError(t, err)
			require.NotEqual(t, revision, changedRevision)
			require.Error(t, ValidateOrdinaryRecoveryJob(job, files, stateSHA), "sort-only edits must not bypass original launch shape fences")
			files[filename] = *catalog
			changedHash, err := OrdinaryCatalogSHA256(files)
			require.NoError(t, err)
			require.NotEqual(t, originalHash, changedHash)
		}
	}
}

func TestOrdinaryRecoveryCheckpointFilenameRequiresFlatIdentityAndValidLSN(t *testing.T) {
	name, err := OrdinaryRecoveryCheckpointFilename("recovery-2", "1A/BC")
	require.NoError(t, err)
	require.Equal(t, "recovery-2-1A-BC.json", name)
	for _, input := range [][2]string{{"", "0/100"}, {".", "0/100"}, {"..", "0/100"}, {"../foreign", "0/100"}, {"parent/child", "0/100"}, {"parent\\child", "0/100"}, {"runtime\x00foreign", "0/100"}, {"runtime", "../state"}, {"runtime", "0/100/1"}, {"runtime", "0/xyz"}, {"runtime", "0/"}} {
		name, err := OrdinaryRecoveryCheckpointFilename(input[0], input[1])
		require.Error(t, err)
		require.Empty(t, name)
	}
}
