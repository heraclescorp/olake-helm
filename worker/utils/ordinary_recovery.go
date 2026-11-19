package utils

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
)

const OrdinaryRecoveryOriginFile = "ordinary-recovery-origin.json"
const OrdinaryRecoveryBaselineFile = "ordinary-recovery-baseline.json"
const OrdinaryRecoveryResultFile = "ordinary-recovery-result.json"

const OrdinaryRecoveryServerFile = "ordinary-recovery-server.json"

type OrdinaryRecoveryServer struct {
	Version          int    `json:"version"`
	OriginalLaunchID string `json:"original_launch_id"`
	SourceSHA256     string `json:"source_sha256"`
	ServerIdentity   string `json:"server_identity"`
	SlotName         string `json:"slot_name"`
}

// Origin is create-only in the stable operation directory. Recovery runtimes
// receive byte-identical copies; neither an activity retry nor an epoch reset
// is allowed to move the trusted lower bound or replace a bound original receipt.
type OrdinaryRecoveryOrigin struct {
	Version               int    `json:"version"`
	OriginalExecutionID   string `json:"original_execution_id"`
	OriginalLaunchID      string `json:"original_launch_id"`
	OriginalReceiptSHA256 string `json:"original_receipt_sha256"` // Empty when no original receipt was available.
	SourceSHA256          string `json:"source_sha256"`
	DestinationSHA256     string `json:"destination_sha256"`
	CatalogSHA256         string `json:"catalog_sha256"`
	BaselineStateSHA256   string `json:"baseline_state_sha256"`
}

type OrdinaryRecoveryTableProof struct {
	Stream          string `json:"stream"`
	TableUUID       string `json:"table_uuid"`
	FenceSnapshotID int64  `json:"fence_snapshot_id"`
	CommittedLSN    string `json:"committed_lsn"`
}

type OrdinaryRecoveryResult struct {
	OrdinaryRecoveryOrigin
	ExecutionID         string                       `json:"execution_id"`
	LaunchID            string                       `json:"launch_id"`
	StateSHA256         string                       `json:"state_sha256"`
	LowerLSN            string                       `json:"lower_lsn"`
	TargetLSN           string                       `json:"target_lsn"`
	Tables              []OrdinaryRecoveryTableProof `json:"tables"`
	ServerIdentity      string                       `json:"server_identity"`
	SlotName            string                       `json:"slot_name"`
	ToastValueSemantics string                       `json:"toast_value_semantics"`
}

func RawSHA256(data string) string {
	digest := sha256.Sum256([]byte(data))
	return hex.EncodeToString(digest[:])
}

// CatalogSHA256 binds the exact file format and bytes, including file names and
// lengths. This framing is also used by the connector, not JSON serialization.
func OrdinaryCatalogSHA256(files map[string]string) (string, error) {
	legacy, l := files[constants.StreamsFile]
	available, a := files[constants.AvailableStreamsFile]
	selected, s := files[constants.SelectedStreamsFile]
	if (l && (a || s)) || (!l && (!a || !s)) || (l && legacy == "") || (a && available == "") || (s && selected == "") {
		return "", fmt.Errorf("ordinary recovery requires exactly a legacy catalog or a complete split catalog")
	}
	h := sha256.New()
	for _, name := range []string{constants.StreamsFile, constants.AvailableStreamsFile, constants.SelectedStreamsFile} {
		if data, ok := files[name]; ok {
			fmt.Fprintf(h, "%s\x00%d\x00%s", name, len(data), data)
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func ReadOrdinaryFrozenConfigs(ctx context.Context, req *types.ExecutionRequest) (map[string]string, error) {
	original := *req
	original.Controlled, original.RuntimeID = true, req.LaunchID
	return ReadOrdinaryRuntimeConfigs(ctx, &original)
}

func ReadOrdinaryRuntimeConfigs(ctx context.Context, req *types.ExecutionRequest) (map[string]string, error) {
	_, dir := GetExecutionDirAndSubDir(req)
	files := make(map[string]string)
	for _, name := range []string{"source.json", "destination.json", constants.StreamsFile, constants.AvailableStreamsFile, constants.SelectedStreamsFile} {
		data, err := storage.ReadFile(ctx, dir, name, false)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && name != "source.json" && name != "destination.json" {
				continue
			}
			return nil, err
		}
		files[name] = data
	}
	if _, err := OrdinaryCatalogSHA256(files); err != nil {
		return nil, err
	}
	return files, nil
}

// Missing receipts permit only recovery admission. A frozen unbound origin may
// observe late known outcomes without treating them as recovery settlement.
func ReadOrdinaryRecoveryOriginalReceipt(ctx context.Context, req *types.ExecutionRequest, allowKnownOutcomes bool) (string, error) {
	original := *req
	original.Controlled, original.RuntimeID = true, req.LaunchID
	_, dir := GetExecutionDirAndSubDir(&original)
	receipt, err := storage.ReadFile(ctx, dir, "execution-result.json", false)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var proof struct {
		Version     int    `json:"version"`
		ExecutionID string `json:"execution_id"`
		LaunchID    string `json:"launch_id"`
		Command     string `json:"command"`
		Outcome     string `json:"outcome"`
		StateSHA256 string `json:"state_sha256"`
	}
	if err := json.Unmarshal([]byte(receipt), &proof); err != nil {
		return "", err
	}
	if proof.Version != 1 || req.LaunchID == "" || proof.ExecutionID != req.LaunchID || proof.LaunchID != req.LaunchID || proof.Command != "sync" {
		return "", fmt.Errorf("ordinary recovery requires the original identity-bound sync receipt when present")
	}
	if proof.Outcome != "unknown" {
		if !allowKnownOutcomes || (proof.Outcome != "settled" && proof.Outcome != "not_started") {
			return "", fmt.Errorf("ordinary recovery original receipt has an unsupported outcome")
		}
		if proof.Outcome == "settled" {
			if len(proof.StateSHA256) != sha256.Size*2 {
				return "", fmt.Errorf("settled original receipt has an invalid state digest")
			}
			for _, digit := range proof.StateSHA256 {
				if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
					return "", fmt.Errorf("settled original receipt has an invalid state digest")
				}
			}
		}
	}
	return receipt, nil
}

func ReadOrdinaryRecoveryOrigin(ctx context.Context, req *types.ExecutionRequest) (OrdinaryRecoveryOrigin, string, map[string]string, error) {
	var origin OrdinaryRecoveryOrigin
	_, dir := GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	data, err := storage.ReadFile(ctx, dir, OrdinaryRecoveryOriginFile, false)
	if err != nil {
		return origin, "", nil, err
	}
	if err := json.Unmarshal([]byte(data), &origin); err != nil {
		return origin, "", nil, err
	}
	if origin.Version != 1 || req.LaunchID == "" || origin.OriginalExecutionID != req.LaunchID || origin.OriginalLaunchID != req.LaunchID {
		return origin, "", nil, fmt.Errorf("ordinary recovery origin does not bind the original launch")
	}
	files, err := ReadOrdinaryFrozenConfigs(ctx, req)
	if err != nil {
		return origin, "", nil, err
	}
	catalog, err := OrdinaryCatalogSHA256(files)
	if err != nil {
		return origin, "", nil, err
	}
	receipt, err := ReadOrdinaryRecoveryOriginalReceipt(ctx, req, origin.OriginalReceiptSHA256 == "")
	if err != nil {
		return origin, "", nil, err
	}
	baseline, err := storage.ReadFile(ctx, dir, OrdinaryRecoveryBaselineFile, false)
	if err != nil {
		return origin, "", nil, err
	}
	stateSHA, err := StateSHA256(baseline)
	if err != nil {
		return origin, "", nil, err
	}
	if (origin.OriginalReceiptSHA256 != "" && (receipt == "" || origin.OriginalReceiptSHA256 != RawSHA256(receipt))) || origin.SourceSHA256 != RawSHA256(files["source.json"]) || origin.DestinationSHA256 != RawSHA256(files["destination.json"]) || origin.CatalogSHA256 != catalog || origin.BaselineStateSHA256 != stateSHA {
		return origin, "", nil, fmt.Errorf("ordinary recovery origin evidence changed")
	}
	if _, _, err := OrdinaryRecoverySelection(files, baseline); err != nil {
		return origin, "", nil, err
	}
	return origin, baseline, files, nil
}

// Compare saved DB configs semantically because PostgreSQL jsonb changes their
// whitespace/key order. Connector inputs themselves always retain raw hashes.
func ValidateOrdinaryRecoveryJob(job types.JobData, files map[string]string, baselineSHA string) error {
	supported, err := job.SupportsAtomicResync()
	if err != nil {
		return err
	}
	if !supported {
		return fmt.Errorf("ordinary recovery is outside the controlled PostgreSQL/REST domain")
	}
	pairs := [][2]string{{job.Source, files["source.json"]}, {job.Destination, files["destination.json"]}}
	if _, split := files[constants.AvailableStreamsFile]; split {
		pairs = append(pairs, [2]string{job.AvailableStreams, files[constants.AvailableStreamsFile]}, [2]string{job.SelectedStreams, files[constants.SelectedStreamsFile]})
	} else {
		if (job.AvailableStreams != "" && job.AvailableStreams != "null") || (job.SelectedStreams != "" && job.SelectedStreams != "null") {
			return fmt.Errorf("saved catalog format changed")
		}
		pairs = append(pairs, [2]string{job.Streams, files[constants.StreamsFile]})
	}
	for _, pair := range pairs {
		a, err := StateSHA256(pair[0])
		if err != nil {
			return err
		}
		b, err := StateSHA256(pair[1])
		if err != nil {
			return err
		}
		if a != b {
			return fmt.Errorf("saved source, destination or catalog changed since original launch")
		}
	}
	sha, err := StateSHA256(job.State)
	if err != nil {
		return err
	}
	if sha != baselineSHA {
		return fmt.Errorf("saved checkpoint changed since ordinary recovery origin")
	}
	return nil
}

func ParseRecoveryLSN(lsn string) (uint64, error) {
	parts := strings.Split(lsn, "/")
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 || len(parts[0]) > 8 || len(parts[1]) > 8 {
		return 0, fmt.Errorf("invalid PostgreSQL LSN %q", lsn)
	}
	hi, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, err
	}
	lo, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, err
	}
	return hi<<32 | lo, nil
}

// Recovery never broadens selection or backfills. Every originally selected
// stream needs a PK, CDC mode and saved completed GLOBAL membership.
func OrdinaryRecoverySelection(files map[string]string, state string) (map[string]bool, string, error) {
	type definition struct {
		Stream struct {
			Name       string   `json:"name"`
			Namespace  string   `json:"namespace"`
			SyncMode   string   `json:"sync_mode"`
			PrimaryKey []string `json:"source_defined_primary_key"`
		} `json:"stream"`
		SyncMode string `json:"sync_mode"`
	}
	type selection struct {
		StreamName string `json:"stream_name"`
		SyncMode   string `json:"sync_mode"`
	}
	var catalog struct {
		Streams  []definition           `json:"streams"`
		Selected map[string][]selection `json:"selected_streams"`
	}
	data := files[constants.StreamsFile]
	if data == "" {
		data = files[constants.AvailableStreamsFile]
	}
	if err := json.Unmarshal([]byte(data), &catalog); err != nil {
		return nil, "", err
	}
	if selected, ok := files[constants.SelectedStreamsFile]; ok {
		var saved struct {
			Selected map[string][]selection `json:"selected_streams"`
		}
		if err := json.Unmarshal([]byte(selected), &saved); err != nil {
			return nil, "", err
		}
		if saved.Selected == nil {
			return nil, "", fmt.Errorf("split catalog has no explicit selection")
		}
		catalog.Selected = saved.Selected
	}
	definitions := make(map[string]definition)
	for _, d := range catalog.Streams {
		id := d.Stream.Namespace + "." + d.Stream.Name
		if d.Stream.Namespace == "" || d.Stream.Name == "" {
			return nil, "", fmt.Errorf("catalog has unidentified stream")
		}
		if _, exists := definitions[id]; exists {
			return nil, "", fmt.Errorf("catalog has duplicate stream %s", id)
		}
		definitions[id] = d
	}
	selected := make(map[string]bool)
	check := func(id, mode string) error {
		d, ok := definitions[id]
		if !ok || selected[id] || len(d.Stream.PrimaryKey) == 0 {
			return fmt.Errorf("ordinary recovery requires unique existing selected streams with primary keys: %s", id)
		}
		if mode == "" {
			mode = d.SyncMode
		}
		if mode == "" {
			mode = d.Stream.SyncMode
		}
		if mode != "cdc" && mode != "strict_cdc" {
			return fmt.Errorf("ordinary recovery requires all selected streams in CDC/strictCDC: %s", id)
		}
		selected[id] = true
		return nil
	}
	if catalog.Selected == nil {
		for id := range definitions {
			if err := check(id, ""); err != nil {
				return nil, "", err
			}
		}
	} else {
		for namespace, streams := range catalog.Selected {
			for _, stream := range streams {
				if err := check(namespace+"."+stream.StreamName, stream.SyncMode); err != nil {
					return nil, "", err
				}
			}
		}
	}
	if len(selected) == 0 {
		return nil, "", fmt.Errorf("ordinary recovery requires a nonempty frozen selection")
	}
	var checkpoint struct {
		Type   string `json:"type"`
		Global struct {
			State struct {
				LSN string `json:"lsn"`
			} `json:"state"`
			Streams []string `json:"streams"`
		} `json:"global"`
	}
	if err := json.Unmarshal([]byte(state), &checkpoint); err != nil {
		return nil, "", err
	}
	if checkpoint.Type != "GLOBAL" {
		return nil, "", fmt.Errorf("ordinary recovery requires saved GLOBAL checkpoint")
	}
	if _, err := ParseRecoveryLSN(checkpoint.Global.State.LSN); err != nil {
		return nil, "", err
	}
	members := make(map[string]bool)
	for _, stream := range checkpoint.Global.Streams {
		members[stream] = true
	}
	for stream := range selected {
		if !members[stream] {
			return nil, "", fmt.Errorf("selected stream %s has no completed baseline membership", stream)
		}
	}
	return selected, checkpoint.Global.State.LSN, nil
}

func ValidateOrdinaryRecoveryServer(data string, origin OrdinaryRecoveryOrigin) (OrdinaryRecoveryServer, error) {
	var server OrdinaryRecoveryServer
	if err := json.Unmarshal([]byte(data), &server); err != nil {
		return server, err
	}
	if server.Version != 1 || server.OriginalLaunchID != origin.OriginalLaunchID || server.SourceSHA256 != origin.SourceSHA256 || server.ServerIdentity == "" || server.SlotName == "" {
		return server, fmt.Errorf("ordinary recovery server pin is not bound to original source/launch")
	}
	return server, nil
}

// Preserve every observed standalone identity pin after exact retirement. Missing
// pins are reconstructed by connector catalog preflight, never by old receipts.
func PreserveOrdinaryRecoveryServer(ctx context.Context, req *types.ExecutionRequest, origin OrdinaryRecoveryOrigin) error {
	var server *OrdinaryRecoveryServer
	_, stableDir := GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	if req.RuntimeID != req.LaunchID {
		_, runtimeDir := GetExecutionDirAndSubDir(req)
		data, err := storage.ReadFile(ctx, runtimeDir, OrdinaryRecoveryServerFile, false)
		if err == nil {
			pin, err := ValidateOrdinaryRecoveryServer(data, origin)
			if err != nil {
				return err
			}
			server = &pin
			if _, err := storage.CreateFileIfAbsent(ctx, stableDir, OrdinaryRecoveryServerFile, data); err != nil {
				return err
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	stored, err := storage.ReadFile(ctx, stableDir, OrdinaryRecoveryServerFile, false)
	if errors.Is(err, fs.ErrNotExist) && server == nil {
		return nil
	}
	if err != nil {
		return err
	}
	prior, err := ValidateOrdinaryRecoveryServer(stored, origin)
	if err != nil {
		return err
	}
	if server != nil && prior != *server {
		return fmt.Errorf("PostgreSQL server identity changed between recovery epochs")
	}
	return nil
}
