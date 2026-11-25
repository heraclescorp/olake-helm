package utils

import (
	"strings"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/stretchr/testify/require"
)

const backfillSelectionCatalog = `{"streams":[{"stream":{"namespace":"public","name":"one","sync_mode":"cdc","source_defined_primary_key":["id"]}}],"selected_streams":{"public":[{"stream_name":"one"}]}}`
const backfillSelectionState = `{"type":"GLOBAL","global":{"state":{"lsn":"0/100"},"streams":[]},"streams":[{"namespace":"public","stream":"one","sync_mode":"cdc","state":{"backfill_table":{"table_uuid":"table-one","shape_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"backfill_plan":[{"min":null,"max":9007199254740993},{"min":9007199254740993,"max":null}],"resume_plan":[{"min":null,"max":9007199254740993},{"min":9007199254740993,"max":null}],"chunks":[{"max":null,"min":9007199254740993}]}}]}`

func TestBackfillRecoverySelectionKeepsCatalogDomainAndExactChunkPlans(t *testing.T) {
	for _, split := range []bool{false, true} {
		files := map[string]string{constants.StreamsFile: backfillSelectionCatalog}
		if split {
			files = map[string]string{constants.AvailableStreamsFile: backfillSelectionCatalog, constants.SelectedStreamsFile: `{"selected_streams":{"public":[{"stream_name":"one"}]}}`}
		}
		selected, lower, err := OrdinaryBackfillRecoverySelection(files, backfillSelectionState)
		require.NoError(t, err)
		require.Equal(t, map[string]bool{"public.one": true}, selected)
		reordered := strings.Replace(backfillSelectionState, `"resume_plan":[{"min":null,"max":9007199254740993},{"min":9007199254740993,"max":null}]`, `"resume_plan":[{"max":null,"min":9007199254740993},{"max":9007199254740993,"min":null}]`, 1)
		_, _, err = OrdinaryBackfillRecoverySelection(files, reordered)
		require.NoError(t, err, "native chunk sets do not promise serialization order")
		require.Equal(t, "0/100", lower)
		_, _, err = OrdinaryRecoverySelection(files, backfillSelectionState)
		require.Error(t, err, "partial candidates cannot weaken the settled CDC selection path")
		unstarted := `{"type":"GLOBAL","global":{"state":{"lsn":"0/100"},"streams":[]},"streams":[]}`
		_, _, err = OrdinaryBackfillRecoverySelection(files, unstarted)
		require.NoError(t, err, "an unstarted selected stream can be initialized only by the fenced connector")
		for _, mutation := range []struct{ from, to string }{
			{`"chunks":[{"max":null,"min":9007199254740993}]`, `"chunks":[{"max":null,"min":9007199254740992}]`},
			{`"resume_plan":[{"min":null,"max":9007199254740993},{"min":9007199254740993,"max":null}]`, `"resume_plan":[{"min":9007199254740993,"max":null}]`},
			{`"backfill_plan":[{"min":null,"max":9007199254740993},{"min":9007199254740993,"max":null}]`, `"backfill_plan":[{"min":null,"max":null}]`},
			{`"backfill_plan":[{"min":null,"max":9007199254740993},{"min":9007199254740993,"max":null}]`, `"backfill_plan":[{"min":null,"max":null},{"min":null,"max":null}]`},
			{`"min":null,"max":9007199254740993`, `"max":9007199254740993`},
			{`"streams":[]`, `"streams":["public.one","public.one"]`},
		} {
			_, _, err := OrdinaryBackfillRecoverySelection(files, strings.Replace(backfillSelectionState, mutation.from, mutation.to, 1))
			require.Error(t, err)
		}
		catalogName := constants.StreamsFile
		if split {
			catalogName = constants.AvailableStreamsFile
		}
		for _, catalog := range []string{
			strings.Replace(backfillSelectionCatalog, `"source_defined_primary_key":["id"]`, `"source_defined_primary_key":[]`, 1),
			strings.Replace(backfillSelectionCatalog, `"cdc"`, `"full_refresh"`, 1),
		} {
			files[catalogName] = catalog
			_, _, err := OrdinaryBackfillRecoverySelection(files, backfillSelectionState)
			require.Error(t, err)
		}
	}
}

func TestBackfillRecoveryPreservesSavedOriginalWALBoundary(t *testing.T) {
	for _, saved := range []string{`{}`, `{"type":"GLOBAL","global":{"state":{"lsn":"0/100"}}}`} {
		require.NoError(t, ValidateBackfillRecoveryLowerBound(saved, backfillSelectionState))
	}
	for _, saved := range []string{
		`{"type":"GLOBAL","global":{"state":{"lsn":"0/FF"}}}`,
		`{"type":"GLOBAL","global":{"state":{"lsn":"0/101"}}}`,
		`{"type":"GLOBAL","global":{"state":{}}}`,
	} {
		require.Error(t, ValidateBackfillRecoveryLowerBound(saved, backfillSelectionState))
	}
	for _, candidate := range []string{
		`{}`,
		`{"type":"GLOBAL","global":{"state":{}}}`,
		`{"type":"STREAM","global":{"state":{"lsn":"0/100"}}}`,
	} {
		require.Error(t, ValidateBackfillRecoveryLowerBound(`{"type":"GLOBAL","global":{"state":{"lsn":"0/100"}}}`, candidate))
	}
}

func TestBackfillRecoveryFinalProofAcceptsOnlySameCompletePlan(t *testing.T) {
	files := map[string]string{constants.StreamsFile: backfillSelectionCatalog}
	final := strings.Replace(backfillSelectionState, `"streams":[]`, `"streams":["public.one"]`, 1)
	final = strings.Replace(final, `"resume_plan":[{"min":null,"max":9007199254740993},{"min":9007199254740993,"max":null}],`, "", 1)
	final = strings.Replace(final, `"chunks":[{"max":null,"min":9007199254740993}]`, `"chunks":[]`, 1)
	final = strings.Replace(final, `"backfill_plan":[{"min":null,"max":9007199254740993},{"min":9007199254740993,"max":null}]`, `"backfill_plan":[{"max":null,"min":9007199254740993},{"max":9007199254740993,"min":null}]`, 1)
	proof := []OrdinaryRecoveryTableProof{{Stream: "public.one", TableUUID: "table-one"}}
	require.NoError(t, ValidateBackfillRecoveryFinalState(files, backfillSelectionState, final, proof))
	changed := strings.Replace(final, `"backfill_plan":[{"max":null,"min":9007199254740993},{"max":9007199254740993,"min":null}]`, `"backfill_plan":[{"min":null,"max":null}]`, 1)
	require.Error(t, ValidateBackfillRecoveryFinalState(files, backfillSelectionState, changed, proof))
}
