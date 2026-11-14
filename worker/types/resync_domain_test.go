package types

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAtomicDomainMatchesCanonicalPostgresRESTCatalogFamily(t *testing.T) {
	for _, catalog := range []string{"rest", "lakekeeper", "nessie", "s3tables", "unity", "polaris", "biglake", "horizon", " S3TABLES "} {
		t.Run(catalog, func(t *testing.T) {
			job := JobData{Driver: "Postgres", DestinationDriver: "ICEBERG", Destination: fmt.Sprintf(`{"type":"ICEBERG","writer":{"catalog_type":%q}}`, catalog)}
			supported, err := job.SupportsAtomicResync()
			require.NoError(t, err)
			require.True(t, supported)
		})
	}
	for _, tc := range []struct{ name, source, destType, config string }{
		{"default Glue", "postgres", "iceberg", `{"type":"ICEBERG","writer":{}}`},
		{"explicit Glue", "postgres", "iceberg", `{"type":"ICEBERG","writer":{"catalog_type":"glue"}}`},
		{"parquet", "postgres", "parquet", `{"type":"PARQUET","writer":{"catalog_type":"rest"}}`},
		{"non Postgres", "mongodb", "iceberg", `{"type":"ICEBERG","writer":{"catalog_type":"rest"}}`},
		{"saved destination type mismatch", "postgres", "parquet", `{"type":"ICEBERG","writer":{"catalog_type":"rest"}}`},
		{"unknown catalog", "postgres", "iceberg", `{"type":"ICEBERG","writer":{"catalog_type":"unrecognized"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			supported, err := (JobData{Driver: tc.source, DestinationDriver: tc.destType, Destination: tc.config}).SupportsAtomicResync()
			require.NoError(t, err)
			require.False(t, supported)
		})
	}
	_, err := (JobData{Driver: "postgres", DestinationDriver: "iceberg", Destination: `{"type":`}).SupportsAtomicResync()
	require.Error(t, err, "malformed canonical config must not become a downgrade decision")
}
func TestControlledDomainCannotBeSuppliedByPublicExecutionJSON(t *testing.T) {
	var req ExecutionRequest
	require.NoError(t, json.Unmarshal([]byte(`{"controlled":true,"runtime_id":"caller-runtime"}`), &req))
	require.False(t, req.Controlled)
}
