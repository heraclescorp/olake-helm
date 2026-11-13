package temporal

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
)

// Deterministic authority for activity input tests. SQL interleavings are tested
// against actual PostgreSQL in database/execution_concurrency_test.go.
type activityTestAuthority struct {
	records    map[string]*database.ExecutionRecord
	owner, run string
}

func (db *activityTestAuthority) AcquireJobExecution(_ context.Context, req *types.ExecutionRequest) error {
	if db.owner != "" && (db.owner != req.WorkflowID || db.run != req.RunID) {
		return database.ErrJobExecutionOwned
	}
	if db.records == nil {
		db.records = map[string]*database.ExecutionRecord{}
	}
	if r := db.records[req.WorkflowID]; r != nil {
		if r.OwnerRunID != req.RunID || r.Phase == "handed_off" || (r.Phase == "closed" && (r.Outcome != "retired" || db.owner != req.WorkflowID || db.run != req.RunID)) {
			return database.ErrExecutionClosed
		}
	} else {
		db.records[req.WorkflowID] = &database.ExecutionRecord{Controlled: true, JobID: req.JobID, WorkflowID: req.WorkflowID, OwnerRunID: req.RunID, Phase: "unadmitted"}
	}
	db.owner, db.run = req.WorkflowID, req.RunID
	return nil
}
func (db *activityTestAuthority) ReleaseJobExecution(_ context.Context, req *types.ExecutionRequest) error {
	if db.owner != req.WorkflowID || db.run != req.RunID {
		return database.ErrJobExecutionOwned
	}
	r := db.records[req.WorkflowID]
	if r == nil || r.Phase != "closed" {
		return database.ErrExecutionClosed
	}
	db.owner, db.run = "", ""
	return nil
}
func (db *activityTestAuthority) GetExecution(_ context.Context, _ int, id string) (*database.ExecutionRecord, error) {
	r := db.records[id]
	if r == nil {
		return nil, sql.ErrNoRows
	}
	return r, nil
}
func (db *activityTestAuthority) AdmitExecution(_ context.Context, req *types.ExecutionRequest, attempt, _ string) (*database.ExecutionRecord, bool, error) {
	r := db.records[req.WorkflowID]
	if r == nil || r.OwnerRunID != req.RunID {
		return nil, false, database.ErrJobExecutionOwned
	}
	if r.Phase != "unadmitted" {
		if r.Phase != "admitted" {
			return r, false, database.ErrExecutionClosed
		}
		return r, false, nil
	}
	// Fixture identities are deterministic so old artifact-focused tests can name
	// their directory, without relying on Temporal's random attempt generation.
	r.Phase = "admitted"
	r.LauncherAttemptID = attempt
	if r.Controlled {
		r.RuntimeID = req.WorkflowID
		if r.LaunchID == "" {
			r.LaunchID = req.WorkflowID
		}
	}
	return r, true, nil
}
func (db *activityTestAuthority) GuardExecution(ctx context.Context, req *types.ExecutionRequest, fn func(context.Context, *database.ExecutionRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if db.owner != req.WorkflowID || db.run != req.RunID {
		return database.ErrJobExecutionOwned
	}
	return fn(ctx, db.records[req.WorkflowID])
}
func (db *activityTestAuthority) CheckLaunch(ctx context.Context, req *types.ExecutionRequest) error {
	return db.GuardExecution(ctx, req, func(ctx context.Context, r *database.ExecutionRecord) error {
		if r.Phase != "admitted" || !req.AllowLaunch || r.LauncherAttemptID != req.LauncherAttemptID {
			return database.ErrExecutionClosed
		}
		return utils.ReadExecutionFence(ctx, req)
	})
}
func (db *activityTestAuthority) RecordExecutionResource(_ context.Context, req *types.ExecutionRequest, id, uid string) error {
	r := db.records[req.WorkflowID]
	if r == nil || r.OwnerRunID != req.RunID {
		return database.ErrJobExecutionOwned
	}
	if r.ResourceID != "" && r.ResourceID != id {
		return database.ErrExecutionClosed
	}
	r.ResourceID, r.ResourceUID = id, uid
	return nil
}
func (db *activityTestAuthority) AdoptExecution(_ context.Context, old, newReq *types.ExecutionRequest) error {
	r := db.records[old.WorkflowID]
	if r == nil || r.OwnerRunID != old.RunID || db.owner != old.WorkflowID || db.run != old.RunID {
		return database.ErrJobExecutionOwned
	}
	retiredWithoutPublication := r.Phase == "closed" && (r.Outcome == "not_launched" || r.Outcome == "retired" || (newReq.Command == types.Resync && r.Outcome == "not_published"))
	if retiredWithoutPublication || (newReq.Command != types.Resync && r.LaunchID != "") {
		if !retiredWithoutPublication && (r.Phase != "closed" || (r.Outcome != "settled" && r.Outcome != "not_started" && r.Outcome != "retired")) {
			return fmt.Errorf("ordinary outcome is not proven")
		}
		r.Outcome = "retired"
	} else {
		r.Phase = "unadmitted"
		if r.LaunchID != "" {
			r.Outcome = "recovery"
		}
		r.RuntimeID = ""
		r.ResourceID = ""
		r.ResourceUID = ""
	}
	r.OwnerRunID = newReq.RunID
	db.run = newReq.RunID
	return nil
}
func (db *activityTestAuthority) BeginHandoff(_ context.Context, req *types.ExecutionRequest, token string) error {
	r := db.records[req.WorkflowID]
	if r == nil || r.Phase != "closed" || r.Outcome != "published" {
		return database.ErrExecutionClosed
	}
	r.Phase = "handoff"
	r.HandoffToken = token
	r.HandoffRunID = req.RunID
	r.HandoffRequestID = req.WorkflowID + "/" + req.RunID + "/atomic-resync-sync"
	r.HandoffActive = true
	return nil
}
func (db *activityTestAuthority) ApplyHandoff(ctx context.Context, req *types.ExecutionRequest, fn func(context.Context, *database.ExecutionRecord) error) error {
	r := db.records[req.WorkflowID]
	if r == nil {
		return database.ErrExecutionClosed
	}
	if r.Phase == "handed_off" {
		return nil
	}
	if err := db.GuardExecution(ctx, req, fn); err != nil {
		return err
	}
	r.Phase = "handed_off"
	db.owner, db.run = "", ""
	return nil
}
