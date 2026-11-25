package executor

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/stretchr/testify/require"
)

func TestEmptyCheckpointFoldersDoNotLoadUnneededBindingAuthority(t *testing.T) {
	req, dir := executorEnrollmentFixture(t)
	ctx := context.Background()
	origin := utils.OrdinaryRecoveryOrigin{Backfill: true}
	config, err := utils.OrdinaryRecoveryCheckpointConfig(ctx, req, origin, "not-a-checkpoint", nil)
	require.NoError(t, err)
	require.Nil(t, config)
	require.NoError(t, utils.CloseExecutionFence(ctx, req))
	checkpoint, err := utils.PreserveOrdinaryRecoveryCheckpoints(ctx, req, origin, "not-a-checkpoint", nil)
	require.NoError(t, err)
	require.Nil(t, checkpoint)
	require.NoError(t, storage.WriteFileAtomic(ctx, dir, utils.OrdinaryRecoveryCheckpointFile, `{}`))
	_, err = utils.PreserveOrdinaryRecoveryCheckpoints(ctx, req, origin, "not-a-checkpoint", nil)
	require.ErrorContains(t, err, "unpreserved checkpoint input")
}
