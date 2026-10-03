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
		s, err := Open(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`TRUNCATE stacks, change_sets, operations, events RESTART IDENTITY`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	})
}

func TestMigrationsAreIdempotent(t *testing.T) {
	dsn := os.Getenv("STRATA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("STRATA_TEST_DATABASE_URL not set")
	}
	for i := 0; i < 2; i++ {
		s, err := Open(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
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
