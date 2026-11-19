package types

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

// CanonicalJSON matches the UI catalog_revision protocol. Empty database text and
// JSON null are the same; object keys sort recursively and number lexemes survive.
func CanonicalJSON(text string) (any, error) {
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("expected exactly one JSON value")
	}
	return value, nil
}

// CatalogRevision must remain identical to the UI/BFF algorithm: SHA256 of the
// canonical JSON object with these four parsed fields, including all settings.
func (j JobData) CatalogRevision() (string, error) {
	fields := map[string]string{
		"streams_config":           j.Streams,
		"available_streams_config": j.AvailableStreams,
		"selected_streams_config":  j.SelectedStreams,
		"advanced_settings":        j.AdvancedSettings,
	}
	values := make(map[string]any, len(fields))
	for key, text := range fields {
		value, err := CanonicalJSON(text)
		if err != nil {
			return "", fmt.Errorf("invalid %s JSON", key)
		}
		values[key] = value
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:]), nil
}

type EnrollmentSchema struct {
	Namespace           string `json:"namespace"`
	DestinationDatabase string `json:"destination_database"`
}

type EnrollmentDefaults struct {
	Normalization      *bool  `json:"normalization"`
	AppendMode         *bool  `json:"append_mode"`
	UpdateType         string `json:"update_type"`
	MergeStrategy      string `json:"merge_strategy,omitempty"`
	SyncNewColumns     *bool  `json:"sync_new_columns"`
	DropRemovedColumns *bool  `json:"drop_removed_columns,omitempty"`
}

type EnrollmentPolicy struct {
	Enabled            bool               `json:"enabled"`
	Schemas            []EnrollmentSchema `json:"schemas"`
	Defaults           EnrollmentDefaults `json:"defaults"`
	TargetQueryEngines []string           `json:"target_query_engines,omitempty"`
}

type EnrollmentID struct {
	Namespace  string `json:"namespace"`
	StreamName string `json:"stream_name"`
}

type EnrollmentSkip struct {
	EnrollmentID
	Reason string `json:"reason"`
}

type EnrollmentResult struct {
	Enrolled         []EnrollmentID   `json:"enrolled"`
	Skipped          []EnrollmentSkip `json:"skipped"`
	Streams          string           `json:"streams_config"`
	AvailableStreams string           `json:"available_streams_config"`
	SelectedStreams  string           `json:"selected_streams_config"`
}

// AutoTableEnrollment parses only the owned setting; unknown settings are never
// round-tripped through this struct when persisting an index requirement.
func (j JobData) AutoTableEnrollment() (*EnrollmentPolicy, error) {
	if strings.TrimSpace(j.AdvancedSettings) == "" {
		return nil, nil
	}
	var settings struct {
		Policy             *EnrollmentPolicy `json:"auto_table_enrollment"`
		TargetQueryEngines []string          `json:"target_query_engines"`
	}
	if err := json.Unmarshal([]byte(j.AdvancedSettings), &settings); err != nil {
		return nil, fmt.Errorf("invalid advanced settings JSON")
	}
	policy := settings.Policy
	if policy == nil || !policy.Enabled {
		return nil, nil
	}
	if !strings.EqualFold(j.Driver, "postgres") || !strings.EqualFold(j.DestinationDriver, "iceberg") {
		return nil, fmt.Errorf("automatic table enrollment requires PostgreSQL and Iceberg")
	}
	defaults := policy.Defaults
	if defaults.Normalization == nil || defaults.AppendMode == nil || defaults.SyncNewColumns == nil ||
		(defaults.UpdateType != "eq" && defaults.UpdateType != "pos" && defaults.UpdateType != "dv") {
		return nil, fmt.Errorf("automatic table enrollment requires explicit valid defaults")
	}
	// An omitted strategy is the historical MOR default. An explicit empty or
	// null value is not a strategy and must not silently become MOR.
	var raw struct {
		Policy struct {
			Defaults map[string]json.RawMessage `json:"defaults"`
		} `json:"auto_table_enrollment"`
	}
	if err := json.Unmarshal([]byte(j.AdvancedSettings), &raw); err != nil {
		return nil, fmt.Errorf("invalid automatic enrollment defaults")
	}
	if _, present := raw.Policy.Defaults["merge_strategy"]; present && defaults.MergeStrategy == "" {
		return nil, fmt.Errorf("automatic enrollment merge_strategy must be merge-on-read or copy-on-write")
	}
	if defaults.MergeStrategy != "" && defaults.MergeStrategy != "merge-on-read" && defaults.MergeStrategy != "copy-on-write" {
		return nil, fmt.Errorf("automatic enrollment merge_strategy must be merge-on-read or copy-on-write")
	}
	if defaults.MergeStrategy == "copy-on-write" {
		if !*defaults.Normalization || *defaults.AppendMode {
			return nil, fmt.Errorf("copy-on-write enrollment requires normalized upsert defaults")
		}
		source, err := decodeCatalog(j.Source)
		if err != nil {
			return nil, fmt.Errorf("copy-on-write enrollment requires valid PostgreSQL source configuration")
		}
		if method, exists := source["update_method"]; exists && method != nil {
			updateMethod, ok := method.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("copy-on-write enrollment requires source-based TOAST recovery")
			}
			if value, exists := updateMethod["toast_value_source"]; exists && value != nil && value != "" && value != "source" {
				return nil, fmt.Errorf("copy-on-write enrollment requires source-based TOAST recovery")
			}
		}
	}
	available := j.Streams
	if j.AvailableStreams != "" && j.SelectedStreams != "" {
		available = j.AvailableStreams
	}
	catalog, err := decodeCatalog(available)
	if err != nil {
		return nil, err
	}
	definitions, err := streamDefinitions(catalog)
	if err != nil {
		return nil, err
	}
	namespaces := make(map[string]bool)
	for id := range definitions {
		namespaces[id.Namespace] = true
	}
	approved := make(map[string]bool)
	if len(policy.Schemas) == 0 {
		return nil, fmt.Errorf("automatic table enrollment requires approved schemas")
	}
	for _, schema := range policy.Schemas {
		if strings.TrimSpace(schema.Namespace) == "" || strings.TrimSpace(schema.DestinationDatabase) == "" ||
			approved[schema.Namespace] || !namespaces[schema.Namespace] {
			return nil, fmt.Errorf("automatic table enrollment schema mappings must be unique, nonblank and present in the saved catalog")
		}
		approved[schema.Namespace] = true
	}
	policy.TargetQueryEngines = settings.TargetQueryEngines
	return policy, nil
}

func decodeCatalog(text string) (map[string]any, error) {
	value, err := CanonicalJSON(text)
	catalog, ok := value.(map[string]any)
	if err != nil || !ok {
		return nil, fmt.Errorf("invalid enrollment catalog JSON object")
	}
	return catalog, nil
}

func streamDefinitions(catalog map[string]any) (map[EnrollmentID]any, error) {
	entries, ok := catalog["streams"].([]any)
	if !ok {
		return nil, fmt.Errorf("enrollment catalog requires streams array")
	}
	result := make(map[EnrollmentID]any, len(entries))
	for _, entry := range entries {
		configured, ok := entry.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid configured stream")
		}
		stream, ok := configured["stream"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid stream definition")
		}
		namespace, _ := stream["namespace"].(string)
		name, _ := stream["name"].(string)
		id := EnrollmentID{Namespace: namespace, StreamName: name}
		if namespace == "" || name == "" || result[id] != nil {
			return nil, fmt.Errorf("blank or duplicate enrollment stream identity")
		}
		result[id] = entry
	}
	return result, nil
}

func selectedDefinitions(catalog map[string]any) (map[EnrollmentID]any, error) {
	groups, ok := catalog["selected_streams"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("automatic additions require explicit saved selected_streams; save the selection first")
	}
	result := make(map[EnrollmentID]any)
	for namespace, group := range groups {
		entries, ok := group.([]any)
		if !ok {
			return nil, fmt.Errorf("invalid selected stream namespace")
		}
		for _, entry := range entries {
			metadata, ok := entry.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid selected stream metadata")
			}
			name, _ := metadata["stream_name"].(string)
			id := EnrollmentID{Namespace: namespace, StreamName: name}
			if namespace == "" || name == "" || result[id] != nil {
				return nil, fmt.Errorf("blank or duplicate selected stream identity")
			}
			result[id] = entry
		}
	}
	return result, nil
}

// ValidateEnrollment accepts only the declared additions, never edits/deletions
// to known definitions, selections, exclusions or unrelated catalog metadata.
func (j JobData) ValidateEnrollment(result EnrollmentResult) (JobData, error) {
	policy, err := j.AutoTableEnrollment()
	if err != nil || policy == nil {
		return JobData{}, fmt.Errorf("automatic enrollment policy is not valid and enabled")
	}
	proposed := j
	split := j.AvailableStreams != "" && j.SelectedStreams != ""
	if (split && (result.Streams != "" || result.AvailableStreams == "" || result.SelectedStreams == "")) ||
		(!split && (result.Streams == "" || result.AvailableStreams != "" || result.SelectedStreams != "")) {
		return JobData{}, fmt.Errorf("enrollment result changed catalog format")
	}
	if split {
		proposed.AvailableStreams, proposed.SelectedStreams = result.AvailableStreams, result.SelectedStreams
	} else {
		proposed.Streams = result.Streams
	}
	if len(result.Enrolled) == 0 {
		before, err := j.CatalogRevision()
		if err != nil {
			return JobData{}, err
		}
		after, err := proposed.CatalogRevision()
		if err != nil || before != after {
			return JobData{}, fmt.Errorf("empty enrollment result changed the catalog")
		}
		return j, nil
	}
	declared := make(map[EnrollmentID]bool, len(result.Enrolled))
	approved := make(map[string]string, len(policy.Schemas))
	for _, schema := range policy.Schemas {
		approved[schema.Namespace] = schema.DestinationDatabase
	}
	for _, id := range result.Enrolled {
		if id.Namespace == "" || id.StreamName == "" || declared[id] || approved[id.Namespace] == "" {
			return JobData{}, fmt.Errorf("enrollment result contains duplicate, blank or unapproved identities")
		}
		declared[id] = true
	}
	oldAvailable, newAvailable := j.Streams, result.Streams
	oldSelected, newSelected := j.Streams, result.Streams
	if split {
		oldAvailable, newAvailable = j.AvailableStreams, result.AvailableStreams
		oldSelected, newSelected = j.SelectedStreams, result.SelectedStreams
	}
	before, err := decodeCatalog(oldAvailable)
	if err != nil {
		return JobData{}, err
	}
	after, err := decodeCatalog(newAvailable)
	if err != nil {
		return JobData{}, err
	}
	if err := preserveCatalogFields(before, after, "streams", !split); err != nil {
		return JobData{}, err
	}
	oldDefinitions, err := streamDefinitions(before)
	if err != nil {
		return JobData{}, err
	}
	newDefinitions, err := streamDefinitions(after)
	if err != nil {
		return JobData{}, err
	}
	if err := onlyDeclaredAdditions(oldDefinitions, newDefinitions, declared); err != nil {
		return JobData{}, err
	}
	beforeSelection, err := decodeCatalog(oldSelected)
	if err != nil {
		return JobData{}, err
	}
	afterSelection, err := decodeCatalog(newSelected)
	if err != nil {
		return JobData{}, err
	}
	if split {
		if err := preserveCatalogFields(beforeSelection, afterSelection, "selected_streams", false); err != nil {
			return JobData{}, err
		}
	}
	oldSelections := make(map[EnrollmentID]any)
	implicitLegacy := !split && beforeSelection["selected_streams"] == nil
	if beforeSelection["selected_streams"] != nil {
		oldSelections, err = selectedDefinitions(beforeSelection)
		if err != nil {
			return JobData{}, err
		}
	}
	newSelections, err := selectedDefinitions(afterSelection)
	if err != nil {
		return JobData{}, err
	}
	if implicitLegacy {
		// Legacy nil selection means all known streams with their effective
		// defaults. Materializing it is safe only if every such stream retains
		// exactly those defaults, including all future columns.
		for id, definition := range oldDefinitions {
			if err := validateImplicitLegacySelection(id, definition, newSelections[id]); err != nil {
				return JobData{}, err
			}
			oldSelections[id] = newSelections[id]
		}
	}
	if err := onlyDeclaredAdditions(oldSelections, newSelections, declared); err != nil {
		return JobData{}, err
	}
	// An empty namespace is also saved selection metadata, not an invitation to remove it.
	if groups, ok := beforeSelection["selected_streams"].(map[string]any); ok {
		for namespace := range groups {
			if _, exists := afterSelection["selected_streams"].(map[string]any)[namespace]; !exists {
				return JobData{}, fmt.Errorf("enrollment removed a saved selection namespace")
			}
		}
	}
	oldGroups, _ := beforeSelection["selected_streams"].(map[string]any)
	for namespace := range afterSelection["selected_streams"].(map[string]any) {
		if _, exists := oldGroups[namespace]; exists {
			continue
		}
		justified := false
		for id := range declared {
			justified = justified || id.Namespace == namespace
		}
		if implicitLegacy {
			for id := range oldDefinitions {
				justified = justified || id.Namespace == namespace
			}
		}
		if !justified {
			return JobData{}, fmt.Errorf("enrollment introduced an unrelated selection namespace")
		}
	}
	for id := range declared {
		stream := newDefinitions[id].(map[string]any)["stream"].(map[string]any)
		metadata := newSelections[id].(map[string]any)
		columns, _ := metadata["selected_columns"].(map[string]any)
		rawDrop, hasDrop := metadata["drop_removed_columns"]
		dropRemovedColumns, validDrop := rawDrop.(bool)
		if hasDrop && !validDrop {
			return JobData{}, fmt.Errorf("enrollment addition has an invalid drop_removed_columns setting")
		}
		if stream["destination_database"] != approved[id.Namespace] ||
			metadata["normalization"] != *policy.Defaults.Normalization ||
			metadata["append_mode"] != *policy.Defaults.AppendMode ||
			metadata["update_type"] != policy.Defaults.UpdateType ||
			columns["sync_new_columns"] != *policy.Defaults.SyncNewColumns ||
			dropRemovedColumns != (policy.Defaults.DropRemovedColumns != nil && *policy.Defaults.DropRemovedColumns) {
			return JobData{}, fmt.Errorf("enrollment addition does not match approved destination and defaults")
		}
		if override, exists := metadata["destination_database"]; exists && override != approved[id.Namespace] {
			return JobData{}, fmt.Errorf("enrollment selection overrides the approved destination")
		}
		strategy, exists := metadata["merge_strategy"]
		if policy.Defaults.MergeStrategy == "copy-on-write" {
			if !exists || strategy != "copy-on-write" {
				return JobData{}, fmt.Errorf("enrollment addition must explicitly use approved copy-on-write strategy")
			}
			if err := validateCOWEnrollment(stream, metadata); err != nil {
				return JobData{}, err
			}
		} else if exists && strategy != "merge-on-read" {
			return JobData{}, fmt.Errorf("enrollment addition does not match approved merge-on-read strategy")
		}
	}
	if policy.Defaults.MergeStrategy != "copy-on-write" && !*policy.Defaults.AppendMode && (policy.Defaults.UpdateType == "pos" || policy.Defaults.UpdateType == "dv") {
		var settings map[string]json.RawMessage
		if err := json.Unmarshal([]byte(j.AdvancedSettings), &settings); err != nil {
			return JobData{}, fmt.Errorf("invalid advanced settings JSON")
		}
		settings["index_required"] = json.RawMessage("true")
		encoded, err := json.Marshal(settings)
		if err != nil {
			return JobData{}, err
		}
		proposed.AdvancedSettings = string(encoded)
	}
	return proposed, nil
}

// The connector supplies discovery metadata; only its source-defined keys,
// not user-selected deduplication columns or OLake metadata, establish COW
// eligibility. Validate the proposal independently before committing it.
func validateCOWEnrollment(stream, metadata map[string]any) error {
	if stream["supports_iceberg_copy_on_write"] != true || stream["sync_mode"] != "cdc" {
		return fmt.Errorf("copy-on-write enrollment requires a COW-capable CDC stream")
	}
	if mode, exists := metadata["sync_mode"]; exists && mode != "" && mode != "cdc" {
		return fmt.Errorf("copy-on-write enrollment cannot override the CDC sync mode")
	}
	if table, ok := stream["destination_table"].(string); !ok || table == "" || table != enrollmentDestinationTable(stream["name"].(string)) {
		return fmt.Errorf("copy-on-write enrollment requires the canonical source table destination")
	} else if override, exists := metadata["destination_table"]; exists && override != "" && override != table {
		return fmt.Errorf("copy-on-write enrollment cannot override the destination table")
	}
	for _, key := range []string{"chunk_column", "partition_regex", "filter", "cursor_field"} {
		if value, exists := metadata[key]; exists && value != "" {
			return fmt.Errorf("copy-on-write enrollment has an unapproved %s override", key)
		}
	}
	if value, exists := metadata["use_source_column_names"]; exists && value != false {
		return fmt.Errorf("copy-on-write enrollment has unapproved column naming")
	}
	for _, key := range []string{"filter_config", "dedup_keys", "sort_order"} {
		if value, exists := metadata[key]; exists && value != nil {
			return fmt.Errorf("copy-on-write enrollment has an unapproved %s override", key)
		}
	}
	schema, _ := stream["type_schema"].(map[string]any)
	properties, _ := schema["properties"].(map[string]any)
	keys, ok := stream["source_defined_primary_key"].([]any)
	if !ok || len(keys) == 0 || len(properties) == 0 {
		return fmt.Errorf("copy-on-write enrollment requires source-defined primary keys and their source schema")
	}
	seen := make(map[string]bool, len(keys))
	for _, value := range keys {
		key, ok := value.(string)
		property, propertyOK := properties[key].(map[string]any)
		types, typesOK := property["type"].([]any)
		if !ok || strings.TrimSpace(key) == "" || seen[key] || !propertyOK || !typesOK || len(types) == 0 {
			return fmt.Errorf("copy-on-write enrollment requires genuine source-defined primary keys")
		}
		if olake, exists := property["olake_column"]; exists && olake != false {
			return fmt.Errorf("copy-on-write enrollment requires genuine source-defined primary keys")
		}
		seen[key] = true
		for _, typ := range types {
			name, valid := typ.(string)
			if !valid || name == "" || name == "null" {
				return fmt.Errorf("copy-on-write enrollment requires non-null source primary keys")
			}
		}
	}
	columns, _ := metadata["selected_columns"].(map[string]any)
	selected, ok := columns["columns"].([]any)
	if !ok || len(selected) != len(properties) {
		return fmt.Errorf("copy-on-write enrollment must select every discovered column")
	}
	selectedNames := make(map[string]bool, len(selected))
	for _, value := range selected {
		name, ok := value.(string)
		if !ok || selectedNames[name] || properties[name] == nil {
			return fmt.Errorf("copy-on-write enrollment must select every discovered column")
		}
		selectedNames[name] = true
	}
	return nil
}

// Match the connector's GenerateDestinationDetails table-name normalization.
func enrollmentDestinationTable(name string) string {
	var result strings.Builder
	for _, symbol := range strings.ToLower(name) {
		if ('a' <= symbol && symbol <= 'z') || ('0' <= symbol && symbol <= '9') {
			result.WriteByte(byte(symbol))
		} else {
			result.WriteByte('_')
		}
	}
	return result.String()
}

func preserveCatalogFields(before, after map[string]any, field string, legacy bool) error {
	for key, value := range before {
		if key == field || (legacy && key == "selected_streams") {
			continue
		}
		if next, exists := after[key]; !exists || !reflect.DeepEqual(value, next) {
			return fmt.Errorf("enrollment changed saved catalog metadata")
		}
	}
	for key := range after {
		if key == field || (legacy && key == "selected_streams") {
			continue
		}
		if _, exists := before[key]; !exists {
			return fmt.Errorf("enrollment introduced unrelated catalog metadata")
		}
	}
	return nil
}

func onlyDeclaredAdditions(before, after map[EnrollmentID]any, declared map[EnrollmentID]bool) error {
	if len(after) != len(before)+len(declared) {
		return fmt.Errorf("enrollment is not an additions-only catalog change")
	}
	for id, value := range before {
		if declared[id] || !reflect.DeepEqual(value, after[id]) {
			return fmt.Errorf("enrollment changed a known stream or selection")
		}
	}
	for id := range after {
		if _, exists := before[id]; !exists && !declared[id] {
			return fmt.Errorf("enrollment introduced an undeclared stream or selection")
		}
	}
	return nil
}

func validateImplicitLegacySelection(id EnrollmentID, definition, selected any) error {
	metadata, ok := selected.(map[string]any)
	if !ok {
		return fmt.Errorf("enrollment removed an implicitly selected legacy stream")
	}
	stream := definition.(map[string]any)["stream"].(map[string]any)
	defaults, _ := stream["default_stream_properties"].(map[string]any)
	normalization, _ := defaults["normalization"].(bool)
	appendMode, _ := defaults["append_mode"].(bool)
	updateType, _ := defaults["update_type"].(string)
	if updateType == "" {
		updateType = "eq"
	}
	if metadata["stream_name"] != id.StreamName || metadata["normalization"] != normalization ||
		metadata["update_type"] != updateType || metadata["selected_columns"] != nil {
		return fmt.Errorf("enrollment changed an implicitly selected legacy stream's effective settings")
	}
	value, present := metadata["append_mode"]
	if (present && value != appendMode) || (!present && appendMode) {
		return fmt.Errorf("enrollment changed an implicitly selected legacy stream's append setting")
	}
	for key, value := range metadata {
		switch key {
		case "stream_name", "normalization", "append_mode", "update_type", "selected_columns":
		case "partition_regex":
			if value != "" {
				return fmt.Errorf("enrollment added a legacy partition override")
			}
		case "use_source_column_names":
			if value != false {
				return fmt.Errorf("enrollment changed legacy column naming")
			}
		default:
			return fmt.Errorf("enrollment added an override to an implicitly selected legacy stream")
		}
	}
	return nil
}
