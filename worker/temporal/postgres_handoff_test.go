package temporal

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

func TestPublishedResyncWorkflowWithJSONBPostgres(t *testing.T) {
	dsn := os.Getenv("OLAKE_ENROLLMENT_TEST_DSN")
	if dsn == "" {
		t.Skip("set OLAKE_ENROLLMENT_TEST_DSN to an isolated localhost PostgreSQL URL")
	}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"postgres", "postgresql"}, u.Scheme)
	require.Contains(t, []string{"localhost", "127.0.0.1", "::1"}, u.Hostname())
	require.NotNil(t, u.User)
	password, _ := u.User.Password()
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	sslmode := u.Query().Get("sslmode")
	if sslmode == "" {
		sslmode = "disable"
	}
	mode := "handofftest" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	settings := map[string]interface{}{
		constants.EnvDatabaseHost: u.Hostname(), constants.EnvDatabasePort: port,
		constants.EnvDatabaseUser: u.User.Username(), constants.EnvDatabasePassword: password,
		constants.EnvDatabaseDatabase: strings.TrimPrefix(u.Path, "/"), constants.EnvDatabaseSSLMode: sslmode,
		constants.EnvDatabaseRunMode: mode, constants.EnvMaxOpenConnections: 4,
	}
	for key, value := range settings {
		old := viper.Get(key)
		viper.Set(key, value)
		t.Cleanup(func() { viper.Set(key, old) })
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	connection, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	job := pq.QuoteIdentifier("olake-" + mode + "-job")
	source := pq.QuoteIdentifier("olake-" + mode + "-source")
	dest := pq.QuoteIdentifier("olake-" + mode + "-destination")
	executions := pq.QuoteIdentifier("olake-" + mode + "-job_execution_records")
	_, err = connection.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (id integer PRIMARY KEY, config text, version text, type text, name text);
		CREATE TABLE %s (id integer PRIMARY KEY, config text, version text, dest_type text, name text);
		CREATE TABLE %s (id integer PRIMARY KEY, name text, streams_config jsonb, available_streams_config jsonb, selected_streams_config jsonb, state jsonb, project_id text, source_id integer, dest_id integer, advanced_settings jsonb, frequency text, created_at timestamptz, updated_at timestamptz, execution_workflow_id text, execution_run_id text, active boolean NOT NULL DEFAULT true);
		CREATE TABLE %s (job_id integer NOT NULL, workflow_id text NOT NULL, owner_run_id text NOT NULL DEFAULT '', launch_id text NOT NULL DEFAULT '', launcher_attempt_id text NOT NULL DEFAULT '', phase text NOT NULL DEFAULT '', runtime_id text NOT NULL DEFAULT '', resource_id text NOT NULL DEFAULT '', resource_uid text NOT NULL DEFAULT '', outcome text NOT NULL DEFAULT '', state_sha256 text NOT NULL DEFAULT '', catalog_sha256 text NOT NULL DEFAULT '', handoff_token text NOT NULL DEFAULT '', handoff_request_id text NOT NULL DEFAULT '', handoff_run_id text NOT NULL DEFAULT '', handoff_active boolean NOT NULL DEFAULT false, controlled boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (job_id,workflow_id))`, source, dest, job, executions))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := connection.ExecContext(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS %s,%s,%s,%s`, executions, job, source, dest))
		require.NoError(t, err)
	})
	_, err = connection.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s VALUES(1,'{}','v-test','postgres','source');
		INSERT INTO %s VALUES(2,'{"type":"ICEBERG","writer":{"catalog_type":"rest"}}','v-test','iceberg','destination');
		INSERT INTO %s(id,name,streams_config,available_streams_config,selected_streams_config,state,project_id,source_id,dest_id,advanced_settings,frequency,created_at) VALUES(42,'job',NULL,'{"streams":[]}','{"selected_streams":{}}','{"old":true}','project',1,2,'{}','15m',now())`, source, dest, job))
	require.NoError(t, err)
	db, err := database.Init(ctx)
	require.NoError(t, err)
	defer db.Close()
	const published = `{"global":{"lsn":9007199254740993},"streams":{"target":{"cursor":"published"}}}`
	workflowID := "sync-project-42-resync-jsonb"
	handle := &resyncScheduleHandle{schedule: client.Schedule{State: &client.ScheduleState{Paused: true}}, loseFirstAck: true}
	remote := &resyncTemporalClient{schedules: &resyncScheduleClient{handle: handle}, service: &resyncServiceClient{patch: handle.patch}}
	a := &Activity{db: db, tempClient: remote, executor: &atomicActivityExecutor{cleanupRequest: func(ctx context.Context, req *types.ExecutionRequest, success bool) error {
		require.True(t, success)
		record, err := db.GetExecution(ctx, req.JobID, req.WorkflowID)
		require.NoError(t, err)
		require.Equal(t, "closed", record.Phase)
		require.Equal(t, "published", record.Outcome)
		return nil
	}}}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: workflowID})
	env.RegisterActivity(a.ResyncActivity)
	env.RegisterActivity(a.PostResyncActivity)
	env.RegisterActivity(a.TriggerSyncAfterResyncActivity)
	env.RegisterActivity(a.ReleaseJobExecutionActivity)
	env.OnActivity(ResyncActivity, mock.Anything, mock.Anything).Return(func(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
		require.NoError(t, db.AcquireJobExecution(ctx, req))
		record, admitted, err := db.AdmitExecution(ctx, req, "attempt-1", "published-runtime")
		require.NoError(t, err)
		require.True(t, admitted)
		req.RuntimeID, req.LaunchID, req.Controlled = record.RuntimeID, record.LaunchID, record.Controlled
		_, err = db.SealExecution(ctx, req)
		require.NoError(t, err)
		require.NoError(t, db.GuardExecution(ctx, req, func(ctx context.Context, _ *database.ExecutionRecord) error {
			return db.UpdateJobState(ctx, req.JobID, req.WorkflowID, req.RunID, published)
		}))
		require.NoError(t, db.FinishExecution(ctx, req, "published"))
		return &types.ExecutorResponse{}, nil
	}).Once()
	req := &types.ExecutionRequest{Command: types.Resync, JobID: 42, ProjectID: "project", Args: atomicArgs(workflowID)}
	env.ExecuteWorkflow(RunSyncWorkflow, req)
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
	record, err := db.GetExecution(ctx, 42, workflowID)
	require.NoError(t, err)
	require.Equal(t, "handed_off", record.Phase)
	require.Equal(t, "published", record.Outcome)
	stateSHA, err := utils.StateSHA256(published)
	require.NoError(t, err)
	require.Equal(t, stateSHA, record.StateSHA256)
	require.Len(t, handle.requestIDs, 2)
	require.Equal(t, handle.requestIDs[0], handle.requestIDs[1])
	require.Equal(t, 1, handle.triggers)
	require.False(t, handle.schedule.State.Paused)
	action, ok := handle.schedule.Action.(*client.ScheduleWorkflowAction)
	require.True(t, ok)
	restored, ok := action.Args[0].(*types.ExecutionRequest)
	require.True(t, ok)
	require.Equal(t, types.Sync, restored.Command)
	require.Equal(t, workflowID, restored.AfterResyncOperationID)
	require.Equal(t, record.HandoffRunID, restored.AfterResyncRunID)
	require.Equal(t, record.HandoffToken, restored.AfterResyncTransferToken)
	require.Equal(t, stateSHA, restored.AfterResyncStateSHA256)
	var actualState string
	require.NoError(t, connection.QueryRowContext(ctx, fmt.Sprintf(`SELECT state FROM %s WHERE id=42`, job)).Scan(&actualState))
	actualSHA, err := utils.StateSHA256(actualState)
	require.NoError(t, err)
	require.Equal(t, stateSHA, actualSHA)
	var workflowOwner, runOwner sql.NullString
	require.NoError(t, connection.QueryRowContext(ctx, fmt.Sprintf(`SELECT execution_workflow_id,execution_run_id FROM %s WHERE id=42`, job)).Scan(&workflowOwner, &runOwner))
	require.False(t, workflowOwner.Valid)
	require.False(t, runOwner.Valid)
}
