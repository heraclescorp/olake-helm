package types

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const enrollmentTestSettings = `{"unknown":{"large":9007199254740993},"index_required":false,"target_query_engines":["snowflake"],"auto_table_enrollment":{"enabled":true,"schemas":[{"namespace":"public","destination_database":"lake:public"}],"defaults":{"normalization":true,"append_mode":false,"update_type":"pos","sync_new_columns":false}}}`
const enrollmentOldDefinition = `{"stream":{"name":"old","namespace":"public","destination_database":"keep","destination_table":"old","default_stream_properties":{"normalization":false,"append_mode":true,"update_type":"eq"}},"unknown":{"retain":9007199254740993}}`
const enrollmentExcludedDefinition = `{"stream":{"name":"excluded","namespace":"public","destination_database":"keep","destination_table":"excluded"}}`
const enrollmentNewDefinition = `{"stream":{"name":"new","namespace":"public","destination_database":"lake:public","destination_table":"new","sync_mode":"cdc"}}`
const enrollmentOldSelection = `{"stream_name":"old","normalization":false,"append_mode":true,"update_type":"eq","filter":"id > 10","unknown":{"retain":true},"selected_columns":{"columns":["id"],"sync_new_columns":true}}`
const enrollmentNewSelection = `{"stream_name":"new","normalization":true,"append_mode":false,"update_type":"pos","selected_columns":{"columns":["id"],"sync_new_columns":false}}`

func enrollmentFixture(split bool) (JobData, EnrollmentResult) {
	job := JobData{Driver: "postgres", DestinationDriver: "iceberg", AdvancedSettings: enrollmentTestSettings,
		State: `{"global":{"lsn":"12345"},"streams":{"public.old":{"cursor":"keep"}}}`}
	oldStreams := `{"streams":[` + enrollmentOldDefinition + `,` + enrollmentExcludedDefinition + `],"root_unknown":{"keep":true}}`
	newStreams := `{"streams":[` + enrollmentOldDefinition + `,` + enrollmentExcludedDefinition + `,` + enrollmentNewDefinition + `],"root_unknown":{"keep":true}}`
	oldSelected := `{"selected_streams":{"public":[` + enrollmentOldSelection + `],"empty":[]},"selection_unknown":7}`
	newSelected := `{"selected_streams":{"public":[` + enrollmentOldSelection + `,` + enrollmentNewSelection + `],"empty":[]},"selection_unknown":7}`
	result := EnrollmentResult{Enrolled: []EnrollmentID{{Namespace: "public", StreamName: "new"}}}
	if split {
		job.AvailableStreams, job.SelectedStreams = oldStreams, oldSelected
		result.AvailableStreams, result.SelectedStreams = newStreams, newSelected
	} else {
		job.Streams = `{"streams":[` + enrollmentOldDefinition + `,` + enrollmentExcludedDefinition + `],"selected_streams":{"public":[` + enrollmentOldSelection + `],"empty":[]},"root_unknown":{"keep":true}}`
		result.Streams = `{"streams":[` + enrollmentOldDefinition + `,` + enrollmentExcludedDefinition + `,` + enrollmentNewDefinition + `],"selected_streams":{"public":[` + enrollmentOldSelection + `,` + enrollmentNewSelection + `],"empty":[]},"root_unknown":{"keep":true}}`
	}
	return job, result
}

func TestCatalogRevisionCanonicalProtocol(t *testing.T) {
	job := JobData{Streams: ` { "b":9007199254740993, "a":{"z":1.00,"x":"<"}} `, AdvancedSettings: ` {"flag":true} `}
	revision, err := job.CatalogRevision()
	require.NoError(t, err)
	canonical := `{"advanced_settings":{"flag":true},"available_streams_config":null,"selected_streams_config":null,"streams_config":{"a":{"x":"\u003c","z":1.00},"b":9007199254740993}}`
	hash := sha256.Sum256([]byte(canonical))
	require.Equal(t, hex.EncodeToString(hash[:]), revision)
	job.Streams = `{"a":{"x":"<","z":1.00},"b":9007199254740993}`
	job.AvailableStreams, job.SelectedStreams = " null ", "  "
	same, err := job.CatalogRevision()
	require.NoError(t, err)
	require.Equal(t, revision, same)
	job.Streams = strings.Replace(job.Streams, "1.00", "1", 1)
	different, err := job.CatalogRevision()
	require.NoError(t, err)
	require.NotEqual(t, revision, different, "number lexemes must not be rounded or normalized")
	job.Streams = `{} {}`
	_, err = job.CatalogRevision()
	require.Error(t, err)
}

func TestEnrollmentAddsOnlyDeclaredNewStreamsAndPreservesState(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "split"}[split], func(t *testing.T) {
			job, result := enrollmentFixture(split)
			proposed, err := job.ValidateEnrollment(result)
			require.NoError(t, err)
			require.Equal(t, job.State, proposed.State)
			index, err := proposed.IndexRequired()
			require.NoError(t, err)
			require.True(t, index)
			require.Contains(t, proposed.AdvancedSettings, "9007199254740993")
			policy, err := proposed.AutoTableEnrollment()
			require.NoError(t, err)
			require.Equal(t, []string{"snowflake"}, policy.TargetQueryEngines)
			// Simulate the next discovery after a commit: all IDs are known and the
			// unchanged result is accepted without duplicating either selection.
			unchanged := EnrollmentResult{Streams: proposed.Streams, AvailableStreams: proposed.AvailableStreams, SelectedStreams: proposed.SelectedStreams}
			retried, err := proposed.ValidateEnrollment(unchanged)
			require.NoError(t, err)
			require.Equal(t, proposed, retried)
		})
	}
}

func TestEnrollmentDropRemovedColumnsMustMatchPolicy(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, policyValue := range []string{"", "false", "true"} {
			for _, selectedValue := range []string{"", "false", "true", `"true"`, "null"} {
				name := map[bool]string{false: "legacy", true: "split"}[split] + "/policy=" + policyValue + "/selection=" + selectedValue
				t.Run(name, func(t *testing.T) {
					job, result := enrollmentFixture(split)
					if policyValue != "" {
						job.AdvancedSettings = strings.Replace(job.AdvancedSettings, `"sync_new_columns":false`, `"sync_new_columns":false,"drop_removed_columns":`+policyValue, 1)
					}
					selection := &result.Streams
					if split {
						selection = &result.SelectedStreams
					}
					if selectedValue != "" {
						newSelection := strings.Replace(enrollmentNewSelection, `"stream_name":"new"`, `"stream_name":"new","drop_removed_columns":`+selectedValue, 1)
						*selection = strings.Replace(*selection, enrollmentNewSelection, newSelection, 1)
					}
					proposed, err := job.ValidateEnrollment(result)
					if selectedValue == `"true"` || selectedValue == "null" || (policyValue == "true") != (selectedValue == "true") {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					require.Equal(t, job.State, proposed.State)
					require.JSONEq(t, job.AdvancedSettings, strings.Replace(proposed.AdvancedSettings, `"index_required":true`, `"index_required":false`, 1))
				})
			}
		}
	}
}

func TestEnrollmentRejectsCatalogEditsAndUndeclaredSelections(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, mutation := range []string{"edit definition", "delete known", "enable excluded", "edit selection", "change root", "wrong schema", "wrong defaults", "extra empty schema", "empty proposal changes", "format switch", "duplicate enrolled"} {
			t.Run(map[bool]string{false: "legacy/", true: "split/"}[split]+mutation, func(t *testing.T) {
				job, result := enrollmentFixture(split)
				available, selected := &result.Streams, &result.Streams
				if split {
					available, selected = &result.AvailableStreams, &result.SelectedStreams
				}
				switch mutation {
				case "edit definition":
					*available = strings.Replace(*available, `"destination_database":"keep"`, `"destination_database":"evil"`, 1)
				case "delete known":
					*available = strings.Replace(*available, enrollmentExcludedDefinition+`,`, "", 1)
				case "enable excluded":
					*selected = strings.Replace(*selected, enrollmentNewSelection, enrollmentNewSelection+`,{"stream_name":"excluded"}`, 1)
				case "edit selection":
					*selected = strings.Replace(*selected, `"filter":"id > 10"`, `"filter":"id < 10"`, 1)
				case "change root":
					*available = strings.Replace(*available, `"keep":true`, `"keep":false`, 1)
				case "wrong schema":
					result.Enrolled[0].Namespace = "unapproved"
				case "wrong defaults":
					*selected = strings.Replace(*selected, `"update_type":"pos"`, `"update_type":"dv"`, 1)
				case "extra empty schema":
					*selected = strings.Replace(*selected, `"empty":[]`, `"empty":[],"unapproved":[]`, 1)
				case "empty proposal changes":
					result.Enrolled = nil
				case "format switch":
					if split {
						result.Streams = `{}`
					} else {
						result.AvailableStreams = `{}`
					}
				case "duplicate enrolled":
					result.Enrolled = append(result.Enrolled, result.Enrolled[0])
				}
				_, err := job.ValidateEnrollment(result)
				require.Error(t, err)
			})
		}
	}
}

func TestLegacyImplicitSelectionMaterializesWithoutChangingEffectiveSettings(t *testing.T) {
	job, result := enrollmentFixture(false)
	job.Streams = `{"streams":[` + enrollmentOldDefinition + `,` + enrollmentExcludedDefinition + `],"root_unknown":{"keep":true}}`
	oldImplicit := `{"stream_name":"old","partition_regex":"","normalization":false,"append_mode":true,"update_type":"eq","use_source_column_names":false,"selected_columns":null}`
	excludedImplicit := `{"stream_name":"excluded","partition_regex":"","normalization":false,"update_type":"eq","use_source_column_names":false,"selected_columns":null}`
	result.Streams = `{"streams":[` + enrollmentOldDefinition + `,` + enrollmentExcludedDefinition + `,` + enrollmentNewDefinition + `],"selected_streams":{"public":[` + oldImplicit + `,` + excludedImplicit + `,` + enrollmentNewSelection + `]},"root_unknown":{"keep":true}}`
	_, err := job.ValidateEnrollment(result)
	require.NoError(t, err)
	result.Streams = strings.Replace(result.Streams, `"selected_columns":null`, `"selected_columns":{"columns":["id"],"sync_new_columns":false}`, 1)
	_, err = job.ValidateEnrollment(result)
	require.Error(t, err, "materializing legacy selection must retain all future columns")
}

func TestEnrollmentPolicyAndIndexDefaults(t *testing.T) {
	job, result := enrollmentFixture(true)
	for _, setting := range []string{"", `{}`, `{"auto_table_enrollment":{"enabled":false}}`} {
		job.AdvancedSettings = setting
		policy, err := job.AutoTableEnrollment()
		require.NoError(t, err)
		require.Nil(t, policy)
	}
	for _, setting := range []string{
		strings.Replace(enrollmentTestSettings, `"sync_new_columns":false`, `"other":false`, 1),
		strings.Replace(enrollmentTestSettings, `"namespace":"public"`, `"namespace":"missing"`, 1),
		strings.Replace(enrollmentTestSettings, `"update_type":"pos"`, `"update_type":"invalid"`, 1),
	} {
		job.AdvancedSettings = setting
		_, err := job.AutoTableEnrollment()
		require.Error(t, err)
	}
	for _, mode := range []string{"eq", "pos", "dv"} {
		for _, appendMode := range []bool{false, true} {
			job, result = enrollmentFixture(true)
			job.AdvancedSettings = strings.Replace(job.AdvancedSettings, `"update_type":"pos"`, `"update_type":"`+mode+`"`, 1)
			result.SelectedStreams = strings.Replace(result.SelectedStreams, `"update_type":"pos"`, `"update_type":"`+mode+`"`, 1)
			if appendMode {
				job.AdvancedSettings = strings.Replace(job.AdvancedSettings, `"append_mode":false`, `"append_mode":true`, 1)
				result.SelectedStreams = strings.Replace(result.SelectedStreams, `"append_mode":false`, `"append_mode":true`, 1)
			}
			proposed, err := job.ValidateEnrollment(result)
			require.NoError(t, err)
			index, err := proposed.IndexRequired()
			require.NoError(t, err)
			require.Equal(t, !appendMode && mode != "eq", index)
			var originalPolicy, proposedPolicy map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(job.AdvancedSettings), &originalPolicy))
			require.NoError(t, json.Unmarshal([]byte(proposed.AdvancedSettings), &proposedPolicy))
			require.JSONEq(t, string(originalPolicy["unknown"]), string(proposedPolicy["unknown"]))
		}
	}
}

func TestSplitNullSelectionKeepsOldStreamsExcludedAndInactiveCatalogUntouched(t *testing.T) {
	job, result := enrollmentFixture(true)
	job.Streams = `{"unused_legacy":{"keep":true}}`
	job.SelectedStreams = `{"selected_streams":null,"selection_unknown":7}`
	result.SelectedStreams = `{"selected_streams":{"public":[` + enrollmentNewSelection + `]},"selection_unknown":7}`
	proposed, err := job.ValidateEnrollment(result)
	require.NoError(t, err)
	require.Equal(t, job.Streams, proposed.Streams)
	require.NotContains(t, proposed.SelectedStreams, `"stream_name":"old"`)
	require.NotContains(t, proposed.SelectedStreams, `"stream_name":"excluded"`)
}

func TestLegacyNullSelectionAndUnsupportedConnectors(t *testing.T) {
	job, result := enrollmentFixture(false)
	job.Streams = `{"streams":[` + enrollmentOldDefinition + `,` + enrollmentExcludedDefinition + `],"selected_streams":null,"root_unknown":{"keep":true}}`
	oldSelection := `{"stream_name":"old","normalization":false,"append_mode":true,"update_type":"eq","selected_columns":null}`
	excludedSelection := `{"stream_name":"excluded","normalization":false,"update_type":"eq","selected_columns":null}`
	result.Streams = `{"streams":[` + enrollmentOldDefinition + `,` + enrollmentExcludedDefinition + `,` + enrollmentNewDefinition + `],"selected_streams":{"public":[` + oldSelection + `,` + excludedSelection + `,` + enrollmentNewSelection + `]},"root_unknown":{"keep":true}}`
	_, err := job.ValidateEnrollment(result)
	require.NoError(t, err)
	job.Driver = "mysql"
	_, err = job.AutoTableEnrollment()
	require.Error(t, err)
	job.Driver, job.DestinationDriver = "postgres", "parquet"
	_, err = job.AutoTableEnrollment()
	require.Error(t, err)
}
