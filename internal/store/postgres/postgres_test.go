package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/internal/store/storetest"
)

// TestConformance runs against a real database when STRATA_TEST_DATABASE_URL
// is set (CI starts a postgres service container). Each subtest gets a clean schema.
func TestConformance(t *testing.T) {
	dsn := os.Getenv("STRATA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("STRATA_TEST_DATABASE_URL not set")
	}
	storetest.Run(t, func(t *testing.T) state.Store {
		return openIsolated(t, dsn)
	})
}

func TestMigrationsAreIdempotent(t *testing.T) {
	dsn := os.Getenv("STRATA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("STRATA_TEST_DATABASE_URL not set")
	}
	isolated, drop, err := IsolatedSchema(context.Background(), dsn, "strata_test_store")
	if err != nil {
		t.Fatal(err)
	}
	defer drop()
	for i := 0; i < 2; i++ {
		s, err := Open(context.Background(), isolated)
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
}

// openIsolated opens a store in a schema of its own, dropped after the test.
func openIsolated(t *testing.T, dsn string) *Store {
	t.Helper()
	isolated, drop, err := IsolatedSchema(context.Background(), dsn, "strata_test_store")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), isolated)
	if err != nil {
		drop()
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); drop() })
	return s
}

func TestDSNFromEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("DB_HOST", "/cloudsql/p:us-central1:strata")
	t.Setenv("DB_PASSWORD", `it's\secret`)
	t.Setenv("DB_NAME", "")
	t.Setenv("DB_USER", "")
	t.Setenv("DB_PORT", "")
	t.Setenv("DB_SSLMODE", "")
	dsn, err := DSNFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	want := `host='/cloudsql/p:us-central1:strata' dbname='strata' user='strata' password='it\'s\\secret' sslmode='disable'`
	if dsn != want {
		t.Fatalf("dsn =\n%s\nwant\n%s", dsn, want)
	}
	if r := redact(dsn); r != `host='/cloudsql/p:us-central1:strata' dbname='strata' user='strata' password=REDACTED` {
		t.Fatalf("redact = %s", r)
	}
}
