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
const enrollmentNewDefinition = `{"stream":{"name":"new","namespace":"public","supports_iceberg_sort_order":true,"destination_database":"lake:public","destination_table":"new","sync_mode":"cdc"}}`
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
	for _, defaultField := range []string{"", `"default_sort_order":[{"column":"created_at","direction":"asc","null_order":"nulls-last"}],`} {
		job, result := enrollmentFixture(false)
		job.Streams = `{` + defaultField + `"streams":[` + enrollmentOldDefinition + `,` + enrollmentExcludedDefinition + `],"root_unknown":{"keep":true}}`
		oldImplicit := `{"stream_name":"old","partition_regex":"","normalization":false,"append_mode":true,"update_type":"eq","use_source_column_names":false,"selected_columns":null}`
		excludedImplicit := `{"stream_name":"excluded","partition_regex":"","normalization":false,"update_type":"eq","use_source_column_names":false,"selected_columns":null}`
		result.Streams = `{` + defaultField + `"streams":[` + enrollmentOldDefinition + `,` + enrollmentExcludedDefinition + `,` + enrollmentNewDefinition + `],"selected_streams":{"public":[` + oldImplicit + `,` + excludedImplicit + `,` + enrollmentNewSelection + `]},"root_unknown":{"keep":true}}`
		_, err := job.ValidateEnrollment(result)
		require.NoError(t, err)
		result.Streams = strings.Replace(result.Streams, `"selected_columns":null`, `"selected_columns":{"columns":["id"],"sync_new_columns":false}`, 1)
		_, err = job.ValidateEnrollment(result)
		require.Error(t, err, "materializing legacy selection must retain all future columns")
	}
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

func TestEnrollmentPreservesSortPoliciesAndNewTablesInherit(t *testing.T) {
	const preset = `[{"column":"created_at","direction":"asc","null_order":"nulls-last"}]`
	const custom = `[{"column":"id","direction":"desc","null_order":"nulls-first"},{"column":"created_at","direction":"asc","null_order":"nulls-last"}]`
	for _, split := range []bool{false, true} {
		for _, defaultOrder := range []string{"", `[]`, preset} {
			for _, override := range []string{"", `[]`, custom} {
				job, result := enrollmentFixture(split)
				before, after := &job.Streams, &result.Streams
				if split {
					before, after = &job.SelectedStreams, &result.SelectedStreams
				}
				for _, catalog := range []*string{before, after} {
					if defaultOrder != "" {
						*catalog = strings.Replace(*catalog, `{`, `{"default_sort_order":`+defaultOrder+`,`, 1)
					}
					if override != "" {
						*catalog = strings.Replace(*catalog, `"stream_name":"old"`, `"stream_name":"old","sort_order":`+override, 1)
					}
				}
				wire, err := json.Marshal(result)
				require.NoError(t, err)
				var decoded EnrollmentResult
				require.NoError(t, json.Unmarshal(wire, &decoded))
				proposed, err := job.ValidateEnrollment(decoded)
				require.NoError(t, err)
				require.Equal(t, result.Streams, proposed.Streams)
				if split {
					require.Equal(t, job.Streams, proposed.Streams)
					require.Equal(t, result.AvailableStreams, proposed.AvailableStreams)
				}
				selected := proposed.Streams
				if split {
					selected = proposed.SelectedStreams
					require.Equal(t, result.SelectedStreams, selected)
				}
				var catalog struct {
					Default json.RawMessage `json:"default_sort_order"`
					Groups  map[string][]struct {
						Name  string          `json:"stream_name"`
						Order json.RawMessage `json:"sort_order"`
					} `json:"selected_streams"`
				}
				require.NoError(t, json.Unmarshal([]byte(selected), &catalog))
				require.Equal(t, defaultOrder, string(catalog.Default))
				for _, stream := range catalog.Groups["public"] {
					if stream.Name == "old" {
						require.Equal(t, override, string(stream.Order))
					} else if stream.Name == "new" {
						require.Empty(t, stream.Order, "enrolled tables must inherit rather than materializing the current default")
					}
				}
				require.Equal(t, job.State, proposed.State)
			}
		}
	}
}

func TestEnrollmentSortPolicyEditsStayFenced(t *testing.T) {
	const preset = `[{"column":"created_at","direction":"asc","null_order":"nulls-last"}]`
	for _, split := range []bool{false, true} {
		for _, mutation := range []string{"remove default", "change default", "remove override", "change override"} {
			t.Run(map[bool]string{false: "legacy/", true: "split/"}[split]+mutation, func(t *testing.T) {
				job, result := enrollmentFixture(split)
				before, after := &job.Streams, &result.Streams
				if split {
					before, after = &job.SelectedStreams, &result.SelectedStreams
				}
				for _, catalog := range []*string{before, after} {
					*catalog = strings.Replace(*catalog, `{`, `{"default_sort_order":`+preset+`,`, 1)
					*catalog = strings.Replace(*catalog, `"stream_name":"old"`, `"stream_name":"old","sort_order":[]`, 1)
				}
				switch mutation {
				case "remove default":
					*after = strings.Replace(*after, `"default_sort_order":`+preset+`,`, "", 1)
				case "change default":
					*after = strings.Replace(*after, `"default_sort_order":`+preset, `"default_sort_order":[]`, 1)
				case "remove override":
					*after = strings.Replace(*after, `,"sort_order":[]`, "", 1)
				case "change override":
					*after = strings.Replace(*after, `"sort_order":[]`, `"sort_order":`+preset, 1)
				}
				_, err := job.ValidateEnrollment(result)
				require.Error(t, err)
			})
		}
	}
}

const enrollmentCOWDefinition = `{"stream":{"name":"new","namespace":"public","supports_iceberg_copy_on_write":true,"source_defined_primary_key":["id"],"type_schema":{"properties":{"id":{"type":["integer"]},"value":{"type":["null","string"]},"_olake_id":{"type":["string"],"olake_column":true}}},"destination_database":"lake:public","destination_table":"new","sync_mode":"cdc"}}`
const enrollmentCOWSelection = `{"stream_name":"new","normalization":true,"append_mode":false,"update_type":"pos","merge_strategy":"copy-on-write","selected_columns":{"columns":["_olake_id","id","value"],"sync_new_columns":false}}`

func enrollmentCOWFixture(split bool) (JobData, EnrollmentResult) {
	job, result := enrollmentFixture(split)
	job.Source = `{"update_method":{"toast_value_source":"source"}}`
	job.Destination = `{"type":"ICEBERG","writer":{"catalog_type":"rest"}}`
	job.AdvancedSettings = strings.Replace(job.AdvancedSettings, `"update_type":"pos"`, `"update_type":"pos","merge_strategy":"copy-on-write"`, 1)
	available, selection := &result.Streams, &result.Streams
	if split {
		available, selection = &result.AvailableStreams, &result.SelectedStreams
	}
	*available = strings.Replace(*available, enrollmentNewDefinition, enrollmentCOWDefinition, 1)
	*selection = strings.Replace(*selection, enrollmentNewSelection, enrollmentCOWSelection, 1)
	return job, result
}

func TestEnrollmentCOWPolicyValidation(t *testing.T) {
	for _, strategy := range []string{`""`, `null`, `"bad"`, `1`} {
		job, _ := enrollmentCOWFixture(true)
		job.AdvancedSettings = strings.Replace(job.AdvancedSettings, `"merge_strategy":"copy-on-write"`, `"merge_strategy":`+strategy, 1)
		_, err := job.AutoTableEnrollment()
		require.Error(t, err, strategy)
	}
	for _, mutation := range []string{"unnormalized", "append", "invalid update type", "destination toast", "invalid toast", "invalid source"} {
		t.Run(mutation, func(t *testing.T) {
			job, _ := enrollmentCOWFixture(true)
			switch mutation {
			case "unnormalized":
				job.AdvancedSettings = strings.Replace(job.AdvancedSettings, `"normalization":true`, `"normalization":false`, 1)
			case "append":
				job.AdvancedSettings = strings.Replace(job.AdvancedSettings, `"append_mode":false`, `"append_mode":true`, 1)
			case "invalid update type":
				job.AdvancedSettings = strings.Replace(job.AdvancedSettings, `"update_type":"pos"`, `"update_type":"bad"`, 1)
			case "destination toast":
				job.Source = `{"update_method":{"toast_value_source":"destination"}}`
			case "invalid toast":
				job.Source = `{"update_method":{"toast_value_source":"invalid"}}`
			case "invalid source":
				job.Source = `[]`
			}
			_, err := job.AutoTableEnrollment()
			require.Error(t, err)
		})
	}
	job, _ := enrollmentCOWFixture(true)
	for _, source := range []string{`{}`, `{"update_method":{}}`, `{"update_method":{"toast_value_source":null}}`, `{"update_method":{"toast_value_source":"source"}}`} {
		job.Source = source
		policy, err := job.AutoTableEnrollment()
		require.NoError(t, err)
		require.Equal(t, "copy-on-write", policy.Defaults.MergeStrategy)
	}
}

func TestEnrollmentCOWPreservesIndexAndExistingSettings(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, update := range []string{"eq", "pos", "dv"} {
			for _, existingIndex := range []bool{false, true} {
				t.Run(map[bool]string{false: "legacy/", true: "split/"}[split]+update+map[bool]string{false: "/no-index", true: "/existing-index"}[existingIndex], func(t *testing.T) {
					job, result := enrollmentCOWFixture(split)
					job.AdvancedSettings = strings.Replace(job.AdvancedSettings, `"update_type":"pos"`, `"update_type":"`+update+`"`, 1)
					if existingIndex {
						job.AdvancedSettings = strings.Replace(job.AdvancedSettings, `"index_required":false`, `"index_required":true`, 1)
					}
					selection := &result.Streams
					if split {
						selection = &result.SelectedStreams
					}
					if existingIndex {
						// A previously selected indexed MOR stream must retain its index
						// and its exact metadata when an index-free COW stream is added.
						oldSelection := &job.Streams
						if split {
							oldSelection = &job.SelectedStreams
						}
						mixed := strings.Replace(enrollmentOldSelection, `"append_mode":true`, `"append_mode":false`, 1)
						mixed = strings.Replace(mixed, `"update_type":"eq"`, `"update_type":"pos"`, 1)
						*oldSelection = strings.Replace(*oldSelection, enrollmentOldSelection, mixed, 1)
						*selection = strings.Replace(*selection, enrollmentOldSelection, mixed, 1)
					}
					*selection = strings.Replace(*selection, enrollmentCOWSelection, strings.Replace(enrollmentCOWSelection, `"update_type":"pos"`, `"update_type":"`+update+`"`, 1), 1)
					proposed, err := job.ValidateEnrollment(result)
					require.NoError(t, err)
					require.Equal(t, job.AdvancedSettings, proposed.AdvancedSettings)
					require.Equal(t, job.State, proposed.State)
					index, err := proposed.IndexRequired()
					require.NoError(t, err)
					require.Equal(t, existingIndex, index)
				})
			}
		}
	}
}

func TestEnrollmentRejectsForgedCOWProposals(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, mutation := range []string{"missing strategy", "wrong strategy", "missing capability", "missing pk", "empty pk", "fake pk", "metadata pk", "nullable pk", "duplicate pk", "wrong mode", "mode override", "wrong table", "table override", "dedup override", "unselected column", "edit old", "missing new columns"} {
			t.Run(map[bool]string{false: "legacy/", true: "split/"}[split]+mutation, func(t *testing.T) {
				job, result := enrollmentCOWFixture(split)
				available, selected := &result.Streams, &result.Streams
				if split {
					available, selected = &result.AvailableStreams, &result.SelectedStreams
				}
				switch mutation {
				case "missing strategy":
					*selected = strings.Replace(*selected, `,"merge_strategy":"copy-on-write"`, "", 1)
				case "wrong strategy":
					*selected = strings.Replace(*selected, `"merge_strategy":"copy-on-write"`, `"merge_strategy":"merge-on-read"`, 1)
				case "missing capability":
					*available = strings.Replace(*available, `"supports_iceberg_copy_on_write":true`, `"supports_iceberg_copy_on_write":false`, 1)
				case "missing pk":
					*available = strings.Replace(*available, `"source_defined_primary_key":["id"],`, "", 1)
				case "empty pk":
					*available = strings.Replace(*available, `"source_defined_primary_key":["id"]`, `"source_defined_primary_key":[]`, 1)
				case "fake pk":
					*available = strings.Replace(*available, `"source_defined_primary_key":["id"]`, `"source_defined_primary_key":["missing"]`, 1)
				case "metadata pk":
					*available = strings.Replace(*available, `"source_defined_primary_key":["id"]`, `"source_defined_primary_key":["_olake_id"]`, 1)
				case "nullable pk":
					*available = strings.Replace(*available, `"id":{"type":["integer"]}`, `"id":{"type":["integer","null"]}`, 1)
				case "duplicate pk":
					*available = strings.Replace(*available, `"source_defined_primary_key":["id"]`, `"source_defined_primary_key":["id","id"]`, 1)
				case "wrong mode":
					*available = strings.Replace(*available, `"sync_mode":"cdc"`, `"sync_mode":"full_refresh"`, 1)
				case "mode override":
					*selected = strings.Replace(*selected, enrollmentCOWSelection, strings.Replace(enrollmentCOWSelection, `"stream_name":"new"`, `"stream_name":"new","sync_mode":"full_refresh"`, 1), 1)
				case "wrong table":
					*available = strings.Replace(*available, `"destination_table":"new"`, `"destination_table":"other"`, 1)
				case "table override":
					*selected = strings.Replace(*selected, enrollmentCOWSelection, strings.Replace(enrollmentCOWSelection, `"stream_name":"new"`, `"stream_name":"new","destination_table":"other"`, 1), 1)
				case "dedup override":
					*selected = strings.Replace(*selected, enrollmentCOWSelection, strings.Replace(enrollmentCOWSelection, `"stream_name":"new"`, `"stream_name":"new","dedup_keys":["id"]`, 1), 1)
				case "unselected column":
					*selected = strings.Replace(*selected, `["_olake_id","id","value"]`, `["_olake_id","id"]`, 1)
				case "edit old":
					*available = strings.Replace(*available, `"destination_database":"keep"`, `"destination_database":"other"`, 1)
				case "missing new columns":
					*selected = strings.Replace(*selected, `"sync_new_columns":false`, `"sync_new_columns":true`, 1)
				}
				_, err := job.ValidateEnrollment(result)
				require.Error(t, err)
			})
		}
	}
}

func TestEnrollmentMORRejectsUnapprovedCOW(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			job, result := enrollmentFixture(split)
			if explicit {
				job.AdvancedSettings = strings.Replace(job.AdvancedSettings, `"update_type":"pos"`, `"update_type":"pos","merge_strategy":"merge-on-read"`, 1)
			}
			selection := &result.Streams
			if split {
				selection = &result.SelectedStreams
			}
			*selection = strings.Replace(*selection, enrollmentNewSelection, strings.Replace(enrollmentNewSelection, `"stream_name":"new"`, `"stream_name":"new","merge_strategy":"copy-on-write"`, 1), 1)
			_, err := job.ValidateEnrollment(result)
			require.Error(t, err)
			*selection = strings.Replace(*selection, `"merge_strategy":"copy-on-write"`, `"merge_strategy":"merge-on-read"`, 1)
			proposed, err := job.ValidateEnrollment(result)
			require.NoError(t, err)
			index, err := proposed.IndexRequired()
			require.NoError(t, err)
			require.True(t, index)
		}
	}
}
