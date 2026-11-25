package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
)

const OrdinaryRecoveryCheckpointFile = "ordinary-recovery-checkpoint.json"
const OrdinaryRecoveryCheckpointsDir = "ordinary-recovery-checkpoints"

type OrdinaryRecoveryCheckpoint struct {
	OrdinaryRecoveryResult
	FinalTargetLSN           string          `json:"final_target_lsn"`
	State                    json.RawMessage `json:"state"`
	PreviousCheckpointSHA256 string          `json:"previous_checkpoint_sha256,omitempty"`
	PreviousExecutionID      string          `json:"previous_execution_id,omitempty"`
}

type ordinaryCheckpointArtifact struct {
	checkpoint OrdinaryRecoveryCheckpoint
	name       string
	data       string
	sha        string
}

type ordinaryCheckpointAuthority struct {
	origin   OrdinaryRecoveryOrigin
	baseline string
	files    map[string]string
	selected map[string]bool
	lower    uint64
	server   OrdinaryRecoveryServer
	bindings map[string]BackfillRecoveryBinding
}

func OrdinaryRecoveryCheckpointFilename(executionID, lsn string) (string, error) {
	if executionID == "" || executionID == "." || executionID == ".." || strings.ContainsAny(executionID, "/\\\x00") {
		return "", fmt.Errorf("invalid checkpoint execution identity")
	}
	if _, err := ParseRecoveryLSN(lsn); err != nil {
		return "", err
	}
	return executionID + "-" + strings.ReplaceAll(lsn, "/", "-") + ".json", nil
}

func newOrdinaryCheckpointAuthority(ctx context.Context, req *types.ExecutionRequest, origin OrdinaryRecoveryOrigin, baseline string, files map[string]string) (ordinaryCheckpointAuthority, error) {
	authority := ordinaryCheckpointAuthority{origin: origin, baseline: baseline, files: files}
	selection := OrdinaryRecoverySelection
	if origin.Backfill {
		selection = OrdinaryBackfillRecoverySelection
	}
	selected, lower, err := selection(files, baseline)
	if err != nil {
		return authority, err
	}
	authority.selected = selected
	authority.lower, err = ParseRecoveryLSN(lower)
	if err != nil {
		return authority, err
	}
	_, stableDir := GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	data, err := storage.ReadFile(ctx, stableDir, OrdinaryRecoveryServerFile, false)
	if errors.Is(err, fs.ErrNotExist) {
		return authority, nil
	}
	if err != nil {
		return authority, err
	}
	authority.server, err = ValidateOrdinaryRecoveryServer(data, origin)
	if err != nil {
		return authority, err
	}
	if origin.Backfill {
		bindingAuthority, err := newBackfillBindingAuthority(ctx, req, origin, baseline, files)
		if err != nil {
			return authority, err
		}
		if authority.server.ServerIdentity != bindingAuthority.candidate.ServerIdentity || authority.server.SlotName != bindingAuthority.candidate.SlotName {
			return authority, fmt.Errorf("ordinary recovery checkpoint server pin differs from immutable backfill candidate")
		}
		authority.bindings = make(map[string]BackfillRecoveryBinding, len(selected))
		for id, stream := range bindingAuthority.original {
			authority.bindings[id] = BackfillRecoveryBinding{TableUUID: stream.State.Table.TableUUID, ShapeSHA256: stream.State.Table.ShapeSHA256, Plan: stream.State.BackfillPlan}
		}
		for stream := range selected {
			data, err := storage.ReadFile(ctx, stableDir, BackfillRecoveryBindingFilename(stream), false)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return authority, err
			}
			binding, err := bindingAuthority.validate(data, stream)
			if err != nil {
				return authority, err
			}
			authority.bindings[stream] = binding
		}
	}
	return authority, nil
}

func (authority ordinaryCheckpointAuthority) validate(name, data string) (ordinaryCheckpointArtifact, error) {
	artifact := ordinaryCheckpointArtifact{name: name, data: data, sha: RawSHA256(data)}
	checkpoint := &artifact.checkpoint
	if err := json.Unmarshal([]byte(data), checkpoint); err != nil {
		return artifact, fmt.Errorf("invalid ordinary recovery checkpoint: %w", err)
	}
	expectedName, err := OrdinaryRecoveryCheckpointFilename(checkpoint.ExecutionID, checkpoint.TargetLSN)
	if err != nil || checkpoint.OrdinaryRecoveryOrigin != authority.origin || checkpoint.ExecutionID == authority.origin.OriginalExecutionID || checkpoint.LaunchID != authority.origin.OriginalLaunchID || name != expectedName {
		return artifact, fmt.Errorf("ordinary recovery checkpoint does not bind immutable origin, runtime or artifact name")
	}
	if authority.server.ServerIdentity == "" || checkpoint.ServerIdentity != authority.server.ServerIdentity || checkpoint.SlotName != authority.server.SlotName {
		return artifact, fmt.Errorf("ordinary recovery checkpoint differs from pinned PostgreSQL server/slot")
	}
	if checkpoint.ToastValueSemantics != "current_source_lookup" && checkpoint.ToastValueSemantics != "destination" {
		return artifact, fmt.Errorf("ordinary recovery checkpoint has unknown TOAST value semantics")
	}
	lower, lowerErr := ParseRecoveryLSN(checkpoint.LowerLSN)
	target, targetErr := ParseRecoveryLSN(checkpoint.TargetLSN)
	final, finalErr := ParseRecoveryLSN(checkpoint.FinalTargetLSN)
	if lowerErr != nil || targetErr != nil || finalErr != nil || lower != authority.lower || target < lower || final < target {
		return artifact, fmt.Errorf("ordinary recovery checkpoint has invalid lower, batch or final boundary")
	}
	stateSHA, err := StateSHA256(string(checkpoint.State))
	if err != nil || stateSHA != checkpoint.StateSHA256 {
		return artifact, fmt.Errorf("ordinary recovery checkpoint state digest differs from embedded state")
	}
	_, stateLSN, err := OrdinaryRecoverySelection(authority.files, string(checkpoint.State))
	if err != nil {
		return artifact, err
	}
	statePosition, err := ParseRecoveryLSN(stateLSN)
	if err != nil || statePosition != target || len(checkpoint.Tables) != len(authority.selected) {
		return artifact, fmt.Errorf("ordinary recovery checkpoint state or table coverage differs from batch boundary")
	}
	if err := authority.validateStateMembership(string(checkpoint.State)); err != nil {
		return artifact, err
	}
	proofs := make(map[string]OrdinaryRecoveryTableProof, len(checkpoint.Tables))
	uuids := make(map[string]bool, len(checkpoint.Tables))
	for _, table := range checkpoint.Tables {
		committed, err := ParseRecoveryLSN(table.CommittedLSN)
		_, duplicate := proofs[table.Stream]
		if err != nil || !authority.selected[table.Stream] || duplicate || table.TableUUID == "" || uuids[table.TableUUID] || table.FenceSnapshotID <= 0 || committed < target || committed > final {
			return artifact, fmt.Errorf("ordinary recovery checkpoint table proof is incomplete, duplicated or outside proven batch/final range")
		}
		proofs[table.Stream], uuids[table.TableUUID] = table, true
	}
	for _, raw := range []string{authority.baseline, string(checkpoint.State)} {
		streams, err := backfillRecoveryStreams(raw)
		if err != nil {
			return artifact, err
		}
		seen := make(map[string]bool, len(streams))
		for _, stream := range streams {
			id := stream.Namespace + "." + stream.Stream
			if !authority.selected[id] {
				continue
			}
			if seen[id] || (stream.State.Table != nil && stream.State.Table.TableUUID != proofs[id].TableUUID) {
				return artifact, fmt.Errorf("ordinary recovery checkpoint table identity differs from retained state")
			}
			seen[id] = true
		}
	}
	if authority.origin.Backfill {
		if err := ValidateBackfillRecoveryFinalState(authority.files, authority.baseline, string(checkpoint.State), checkpoint.Tables); err != nil {
			return artifact, err
		}
		streams, err := backfillRecoveryStreams(string(checkpoint.State))
		if err != nil {
			return artifact, err
		}
		for _, stream := range streams {
			binding, exists := authority.bindings[stream.Namespace+"."+stream.Stream]
			if !exists || stream.State.Table == nil || binding.TableUUID != stream.State.Table.TableUUID || binding.ShapeSHA256 != stream.State.Table.ShapeSHA256 || !sameBackfillRecoveryPlan(binding.Plan, stream.State.BackfillPlan) {
				return artifact, fmt.Errorf("ordinary recovery checkpoint differs from immutable backfill table binding journal")
			}
		}
	}
	if (checkpoint.PreviousCheckpointSHA256 == "") != (checkpoint.PreviousExecutionID == "") || (checkpoint.PreviousCheckpointSHA256 != "" && !validRecoverySHA256(checkpoint.PreviousCheckpointSHA256)) {
		return artifact, fmt.Errorf("ordinary recovery checkpoint predecessor binding is incomplete")
	}
	return artifact, nil
}

func (authority ordinaryCheckpointAuthority) validateStateMembership(data string) error {
	type checkpointState struct {
		Global struct {
			Streams []string `json:"streams"`
		} `json:"global"`
		Streams []json.RawMessage `json:"streams"`
	}
	var baseline, current checkpointState
	if err := json.Unmarshal([]byte(authority.baseline), &baseline); err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(data), &current); err != nil {
		return err
	}
	expected := make(map[string]bool, len(baseline.Global.Streams)+len(authority.selected))
	for _, stream := range baseline.Global.Streams {
		expected[stream] = true
	}
	for stream := range authority.selected {
		expected[stream] = true
	}
	members := make(map[string]bool, len(current.Global.Streams))
	for _, stream := range current.Global.Streams {
		if !expected[stream] || members[stream] {
			return fmt.Errorf("ordinary recovery checkpoint completed membership is duplicated or newly foreign")
		}
		members[stream] = true
	}
	if len(members) != len(expected) {
		return fmt.Errorf("ordinary recovery checkpoint lacks selected or retained baseline membership")
	}
	unselected := func(streams []json.RawMessage) (map[string]string, error) {
		retained := make(map[string]string)
		seen := make(map[string]bool)
		for _, raw := range streams {
			var stream struct {
				Name      string `json:"stream"`
				Namespace string `json:"namespace"`
			}
			if err := json.Unmarshal(raw, &stream); err != nil {
				return nil, err
			}
			id := stream.Namespace + "." + stream.Name
			if stream.Namespace == "" || stream.Name == "" || seen[id] {
				return nil, fmt.Errorf("ordinary recovery checkpoint contains unidentified or duplicate retained stream state")
			}
			seen[id] = true
			if !authority.selected[id] {
				sha, err := StateSHA256(string(raw))
				if err != nil {
					return nil, err
				}
				retained[id] = sha
			}
		}
		return retained, nil
	}
	oldState, err := unselected(baseline.Streams)
	if err != nil {
		return err
	}
	newState, err := unselected(current.Streams)
	if err != nil {
		return err
	}
	if len(oldState) != len(newState) {
		return fmt.Errorf("ordinary recovery checkpoint changed unselected retained state membership")
	}
	for id, sha := range oldState {
		if newState[id] != sha {
			return fmt.Errorf("ordinary recovery checkpoint changed unselected retained stream state")
		}
	}
	return nil
}

func readOrdinaryCheckpointArtifacts(ctx context.Context, dir string, authority ordinaryCheckpointAuthority, names []string) ([]ordinaryCheckpointArtifact, error) {
	var artifacts []ordinaryCheckpointArtifact
	for _, name := range names {
		if strings.HasPrefix(name, ".") && strings.Contains(name, ".tmp-") {
			continue
		}
		if !strings.HasSuffix(name, ".json") {
			return nil, fmt.Errorf("unexpected ordinary recovery checkpoint artifact %q", name)
		}
		data, err := storage.ReadFile(ctx, dir, path.Join(OrdinaryRecoveryCheckpointsDir, name), false)
		if err != nil {
			return nil, err
		}
		artifact, err := authority.validate(name, data)
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, nil
}

func orderOrdinaryCheckpoints(artifacts []ordinaryCheckpointArtifact) ([]ordinaryCheckpointArtifact, error) {
	bySHA := make(map[string]ordinaryCheckpointArtifact, len(artifacts))
	children := make(map[string]string, len(artifacts))
	root := ""
	for _, artifact := range artifacts {
		if _, duplicate := bySHA[artifact.sha]; duplicate {
			return nil, fmt.Errorf("ordinary recovery checkpoint chain contains duplicate artifacts")
		}
		bySHA[artifact.sha] = artifact
		previous := artifact.checkpoint.PreviousCheckpointSHA256
		if previous == "" {
			if root != "" {
				return nil, fmt.Errorf("ordinary recovery checkpoint chain has conflicting roots")
			}
			root = artifact.sha
		} else {
			if children[previous] != "" {
				return nil, fmt.Errorf("ordinary recovery checkpoint chain forks")
			}
			children[previous] = artifact.sha
		}
	}
	var ordered []ordinaryCheckpointArtifact
	for sha := root; sha != ""; sha = children[sha] {
		artifact, exists := bySHA[sha]
		if !exists || len(ordered) >= len(artifacts) {
			return nil, fmt.Errorf("ordinary recovery checkpoint chain is cyclic or incomplete")
		}
		if len(ordered) > 0 {
			prior := ordered[len(ordered)-1].checkpoint
			current := artifact.checkpoint
			oldTarget, _ := ParseRecoveryLSN(prior.TargetLSN)
			newTarget, _ := ParseRecoveryLSN(current.TargetLSN)
			oldFinal, _ := ParseRecoveryLSN(prior.FinalTargetLSN)
			newFinal, _ := ParseRecoveryLSN(current.FinalTargetLSN)
			if current.PreviousExecutionID != prior.ExecutionID || newTarget < oldTarget || (newTarget == oldTarget && newTarget != newFinal) || newFinal != oldFinal || current.ToastValueSemantics != prior.ToastValueSemantics {
				return nil, fmt.Errorf("ordinary recovery checkpoint chain regresses or changes its fixed target/semantics")
			}
			proofs := make(map[string]OrdinaryRecoveryTableProof, len(prior.Tables))
			for _, table := range prior.Tables {
				proofs[table.Stream] = table
			}
			for _, table := range current.Tables {
				previous := proofs[table.Stream]
				oldPosition, _ := ParseRecoveryLSN(previous.CommittedLSN)
				newPosition, _ := ParseRecoveryLSN(table.CommittedLSN)
				if previous.TableUUID != table.TableUUID || newPosition < oldPosition {
					return nil, fmt.Errorf("ordinary recovery checkpoint chain replaces or regresses a table proof")
				}
			}
		}
		ordered = append(ordered, artifact)
	}
	if len(ordered) != len(artifacts) {
		return nil, fmt.Errorf("ordinary recovery checkpoint chain has missing predecessors")
	}
	return ordered, nil
}

func OrdinaryRecoveryCheckpointConfig(ctx context.Context, req *types.ExecutionRequest, origin OrdinaryRecoveryOrigin, baseline string, files map[string]string) (*types.JobConfig, error) {
	_, stableDir := GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	names, err := storage.ListFiles(ctx, stableDir, OrdinaryRecoveryCheckpointsDir)
	if err != nil || len(names) == 0 {
		return nil, err
	}
	authority, err := newOrdinaryCheckpointAuthority(ctx, req, origin, baseline, files)
	if err != nil {
		return nil, err
	}
	artifacts, err := readOrdinaryCheckpointArtifacts(ctx, stableDir, authority, names)
	if err != nil {
		return nil, err
	}
	ordered, err := orderOrdinaryCheckpoints(artifacts)
	if err != nil || len(ordered) == 0 {
		return nil, err
	}
	return &types.JobConfig{Name: OrdinaryRecoveryCheckpointFile, Data: ordered[len(ordered)-1].data}, nil
}

func PreserveOrdinaryRecoveryCheckpoints(ctx context.Context, req *types.ExecutionRequest, origin OrdinaryRecoveryOrigin, baseline string, files map[string]string) (*OrdinaryRecoveryCheckpoint, error) {
	if err := RequireClosedExecutionFence(ctx, req); err != nil {
		return nil, fmt.Errorf("ordinary recovery checkpoint custody requires positively retired runtime: %w", err)
	}
	_, stableDir := GetWorkflowDirAndSubDir(req.WorkflowID, types.Sync)
	_, runtimeDir := GetExecutionDirAndSubDir(req)
	input, inputErr := storage.ReadFile(ctx, runtimeDir, OrdinaryRecoveryCheckpointFile, false)
	stableNames, err := storage.ListFiles(ctx, stableDir, OrdinaryRecoveryCheckpointsDir)
	if err != nil {
		return nil, err
	}
	runtimeNames, err := storage.ListFiles(ctx, runtimeDir, OrdinaryRecoveryCheckpointsDir)
	if err != nil {
		return nil, err
	}
	if len(stableNames) == 0 && len(runtimeNames) == 0 {
		if inputErr == nil {
			return nil, fmt.Errorf("ordinary recovery runtime has an unpreserved checkpoint input")
		}
		if !errors.Is(inputErr, fs.ErrNotExist) {
			return nil, inputErr
		}
		return nil, nil
	}
	authority, err := newOrdinaryCheckpointAuthority(ctx, req, origin, baseline, files)
	if err != nil {
		return nil, err
	}
	stable, err := readOrdinaryCheckpointArtifacts(ctx, stableDir, authority, stableNames)
	if err != nil {
		return nil, err
	}
	if _, err := orderOrdinaryCheckpoints(stable); err != nil {
		return nil, err
	}
	runtime, err := readOrdinaryCheckpointArtifacts(ctx, runtimeDir, authority, runtimeNames)
	if err != nil {
		return nil, err
	}
	byName := make(map[string]ordinaryCheckpointArtifact, len(stable)+len(runtime))
	for _, artifact := range stable {
		byName[artifact.name] = artifact
	}
	for _, artifact := range runtime {
		if artifact.checkpoint.ExecutionID != req.RuntimeID || req.RuntimeID == req.LaunchID {
			return nil, fmt.Errorf("ordinary recovery checkpoint is not emitted by the retired recovery runtime")
		}
		if prior, exists := byName[artifact.name]; exists && prior.data != artifact.data {
			return nil, fmt.Errorf("immutable ordinary recovery checkpoint conflicts with retired runtime bytes")
		}
		byName[artifact.name] = artifact
	}
	all := make([]ordinaryCheckpointArtifact, 0, len(byName))
	for _, artifact := range byName {
		all = append(all, artifact)
	}
	ordered, err := orderOrdinaryCheckpoints(all)
	if err != nil {
		return nil, err
	}
	var inherited *ordinaryCheckpointArtifact
	currentSeen := false
	for i := range ordered {
		if ordered[i].checkpoint.ExecutionID == req.RuntimeID {
			currentSeen = true
		} else {
			if currentSeen {
				return nil, fmt.Errorf("retired ordinary recovery runtime is stale relative to preserved successors")
			}
			inherited = &ordered[i]
		}
	}
	if inherited == nil {
		if inputErr == nil {
			return nil, fmt.Errorf("ordinary recovery runtime has an unpreserved checkpoint input")
		}
		if !errors.Is(inputErr, fs.ErrNotExist) {
			return nil, inputErr
		}
	} else if inputErr != nil || input != inherited.data {
		return nil, fmt.Errorf("ordinary recovery runtime lacks its exact inherited immutable checkpoint input")
	}
	if len(ordered) == 0 {
		return nil, nil
	}
	if len(runtime) != 0 || inherited != nil {
		if err := validateOrdinaryCheckpointRuntime(ctx, req, authority); err != nil {
			return nil, err
		}
	}
	if origin.Backfill {
		for _, artifact := range runtime {
			if err := ValidateBackfillRecoveryBindingProofs(ctx, req, origin, baseline, files, string(artifact.checkpoint.State)); err != nil {
				return nil, err
			}
		}
	}
	for _, artifact := range ordered {
		if artifact.checkpoint.ExecutionID != req.RuntimeID {
			continue
		}
		name := path.Join(OrdinaryRecoveryCheckpointsDir, artifact.name)
		if _, err := storage.CreateFileIfAbsent(ctx, stableDir, name, artifact.data); err != nil {
			return nil, err
		}
		stored, err := storage.ReadFile(ctx, stableDir, name, false)
		if err != nil {
			return nil, err
		}
		if stored != artifact.data {
			return nil, fmt.Errorf("immutable ordinary recovery checkpoint publication conflicts with original bytes")
		}
	}
	latest := ordered[len(ordered)-1].checkpoint
	return &latest, nil
}

func validateOrdinaryCheckpointRuntime(ctx context.Context, req *types.ExecutionRequest, authority ordinaryCheckpointAuthority) error {
	_, dir := GetExecutionDirAndSubDir(req)
	data, err := storage.ReadFile(ctx, dir, OrdinaryRecoveryOriginFile, false)
	if err != nil {
		return err
	}
	var origin OrdinaryRecoveryOrigin
	if err := json.Unmarshal([]byte(data), &origin); err != nil || origin != authority.origin {
		return fmt.Errorf("ordinary recovery checkpoint runtime origin differs from immutable origin")
	}
	baseline, err := storage.ReadFile(ctx, dir, OrdinaryRecoveryBaselineFile, false)
	if err != nil {
		return err
	}
	sha, err := StateSHA256(baseline)
	if err != nil || sha != authority.origin.BaselineStateSHA256 {
		return fmt.Errorf("ordinary recovery checkpoint runtime baseline differs from immutable origin")
	}
	files, err := ReadOrdinaryRuntimeConfigs(ctx, req)
	if err != nil {
		return err
	}
	catalog, err := OrdinaryCatalogSHA256(files)
	if err != nil {
		return err
	}
	if RawSHA256(files["source.json"]) != origin.SourceSHA256 || RawSHA256(files["destination.json"]) != origin.DestinationSHA256 || catalog != origin.CatalogSHA256 {
		return fmt.Errorf("ordinary recovery checkpoint runtime source, destination or catalog changed")
	}
	if origin.Backfill {
		candidate, err := storage.ReadFile(ctx, dir, BackfillRecoveryCheckpointFile, false)
		if err != nil {
			return err
		}
		if RawSHA256(candidate) != origin.BackfillCheckpointSHA256 {
			return fmt.Errorf("ordinary recovery checkpoint runtime candidate differs from immutable origin")
		}
	}
	return nil
}
