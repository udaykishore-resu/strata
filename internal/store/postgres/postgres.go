// Package postgres is the production state.Store, backed by PostgreSQL
// (Cloud SQL in production). Documents are stored as JSONB with the fields
// needed for concurrency control (version, lease) in dedicated columns.
// Lease times use the database clock so worker clock skew cannot break
// mutual exclusion.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq" // driver

	"github.com/udaykishore-resu/strata/internal/state"
)

// Store is a PostgreSQL-backed state.Store.
type Store struct {
	db *sql.DB
}

// DSNFromEnv builds a connection string from DATABASE_URL, or from the
// discrete DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD / DB_SSLMODE
// variables (DB_HOST may be a Cloud SQL unix socket directory such as
// /cloudsql/project:region:instance).
func DSNFromEnv() (string, error) {
	if u := os.Getenv("DATABASE_URL"); u != "" {
		return u, nil
	}
	host := os.Getenv("DB_HOST")
	if host == "" {
		return "", errors.New("set DATABASE_URL or DB_HOST/DB_NAME/DB_USER/DB_PASSWORD")
	}
	kv := map[string]string{
		"host":     host,
		"port":     os.Getenv("DB_PORT"),
		"dbname":   envOr("DB_NAME", "strata"),
		"user":     envOr("DB_USER", "strata"),
		"password": os.Getenv("DB_PASSWORD"),
		"sslmode":  os.Getenv("DB_SSLMODE"),
	}
	if kv["sslmode"] == "" {
		if strings.HasPrefix(host, "/") {
			kv["sslmode"] = "disable" // unix socket via the Cloud SQL connector is already encrypted
		} else {
			kv["sslmode"] = "require"
		}
	}
	var parts []string
	for _, k := range []string{"host", "port", "dbname", "user", "password", "sslmode"} {
		if v := kv[k]; v != "" {
			parts = append(parts, k+"='"+strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v)+"'")
		}
	}
	return strings.Join(parts, " "), nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Open connects, verifies connectivity and applies migrations.
func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(pctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to postgres (%s): %w", redact(dsn), err)
	}
	s := &Store{db: db}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func redact(dsn string) string {
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		u.User = url.User(u.User.Username())
		return u.String()
	}
	if i := strings.Index(dsn, "password="); i >= 0 {
		return dsn[:i] + "password=REDACTED"
	}
	return dsn
}

// Truncate deletes all data (tests only).
func (s *Store) Truncate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `TRUNCATE stacks, change_sets, operations, events RESTART IDENTITY`)
	return err
}

// Close closes the connection pool.
func (s *Store) Close() error { return s.db.Close() }

var migrations = []string{
	`CREATE TABLE stacks (
		name        text PRIMARY KEY,
		version     bigint NOT NULL,
		status      text NOT NULL,
		doc         jsonb NOT NULL,
		created_at  timestamptz NOT NULL DEFAULT now(),
		updated_at  timestamptz NOT NULL DEFAULT now()
	);
	CREATE TABLE change_sets (
		id          text PRIMARY KEY,
		stack       text NOT NULL,
		status      text NOT NULL,
		doc         jsonb NOT NULL,
		created_at  timestamptz NOT NULL DEFAULT now()
	);
	CREATE INDEX change_sets_stack_idx ON change_sets (stack, created_at DESC);
	CREATE TABLE operations (
		id             text PRIMARY KEY,
		stack          text NOT NULL,
		status         text NOT NULL,
		lease_owner    text NOT NULL DEFAULT '',
		lease_expires  timestamptz,
		attempts       integer NOT NULL DEFAULT 0,
		doc            jsonb NOT NULL,
		created_at     timestamptz NOT NULL DEFAULT now(),
		updated_at     timestamptz NOT NULL DEFAULT now()
	);
	CREATE INDEX operations_runnable_idx ON operations (created_at) WHERE status IN ('PENDING', 'RUNNING');
	CREATE INDEX operations_stack_idx ON operations (stack, created_at DESC);
	CREATE TABLE events (
		id            bigserial PRIMARY KEY,
		stack         text NOT NULL,
		operation_id  text NOT NULL DEFAULT '',
		ts            timestamptz NOT NULL,
		doc           jsonb NOT NULL
	);
	CREATE INDEX events_stack_idx ON events (stack, id);`,
}

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Serialize concurrent instances starting up at the same time.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(727163)`); err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	var current int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return err
	}
	for i := current; i < len(migrations); i++ {
		if _, err := tx.ExecContext(ctx, migrations[i]); err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, i+1); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *Store) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// ------------------------------------------------------------------ stacks

func (s *Store) GetStack(ctx context.Context, name string) (*state.Stack, error) {
	var doc []byte
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT doc, version FROM stacks WHERE name = $1`, name).Scan(&doc, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, state.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var st state.Stack
	if err := json.Unmarshal(doc, &st); err != nil {
		return nil, fmt.Errorf("decode stack %s: %w", name, err)
	}
	st.Version = version
	return &st, nil
}

func (s *Store) ListStacks(ctx context.Context) ([]*state.Stack, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT doc, version FROM stacks ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*state.Stack
	for rows.Next() {
		var doc []byte
		var version int64
		if err := rows.Scan(&doc, &version); err != nil {
			return nil, err
		}
		var st state.Stack
		if err := json.Unmarshal(doc, &st); err != nil {
			return nil, err
		}
		st.Version = version
		out = append(out, &st)
	}
	return out, rows.Err()
}

func saveStack(ctx context.Context, q execer, st *state.Stack) error {
	now := time.Now().UTC()
	prev := st.Version
	next := *st
	next.Version = prev + 1
	next.UpdatedAt = now
	if prev == 0 {
		next.CreatedAt = now
	}
	doc, err := json.Marshal(&next)
	if err != nil {
		return err
	}
	var res sql.Result
	if prev == 0 {
		res, err = q.ExecContext(ctx, `INSERT INTO stacks (name, version, status, doc) VALUES ($1, 1, $2, $3)
			ON CONFLICT (name) DO NOTHING`, st.Name, string(st.Status), string(doc))
	} else {
		res, err = q.ExecContext(ctx, `UPDATE stacks SET version = version + 1, status = $2, doc = $3, updated_at = now()
			WHERE name = $1 AND version = $4`, st.Name, string(st.Status), string(doc), prev)
	}
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return state.ErrConflict
	}
	st.Version, st.UpdatedAt, st.CreatedAt = next.Version, next.UpdatedAt, next.CreatedAt
	return nil
}

func (s *Store) SaveStack(ctx context.Context, st *state.Stack) error {
	return saveStack(ctx, s.db, st)
}

func (s *Store) DeleteStack(ctx context.Context, name string, version int64) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM stacks WHERE name = $1 AND version = $2`, name, version)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return state.ErrConflict
	}
	return nil
}

// ------------------------------------------------------------- change sets

func (s *Store) CreateChangeSet(ctx context.Context, cs *state.ChangeSet) error {
	doc, err := json.Marshal(cs)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO change_sets (id, stack, status, doc, created_at) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO NOTHING`, cs.ID, cs.Stack, string(cs.Status), string(doc), cs.CreatedAt)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return state.ErrConflict
	}
	return nil
}

func (s *Store) GetChangeSet(ctx context.Context, id string) (*state.ChangeSet, error) {
	var doc []byte
	err := s.db.QueryRowContext(ctx, `SELECT doc FROM change_sets WHERE id = $1`, id).Scan(&doc)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, state.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var cs state.ChangeSet
	if err := json.Unmarshal(doc, &cs); err != nil {
		return nil, err
	}
	return &cs, nil
}

func (s *Store) SaveChangeSet(ctx context.Context, cs *state.ChangeSet) error {
	doc, err := json.Marshal(cs)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE change_sets SET status = $2, doc = $3 WHERE id = $1`, cs.ID, string(cs.Status), string(doc))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return state.ErrNotFound
	}
	return nil
}

// -------------------------------------------------------------- operations

const opColumns = `doc, status, lease_owner, lease_expires, attempts, created_at, updated_at`

func scanOp(row interface{ Scan(...any) error }) (*state.Operation, error) {
	var (
		doc          []byte
		status       string
		owner        string
		expires      sql.NullTime
		attempts     int
		created, upd time.Time
	)
	if err := row.Scan(&doc, &status, &owner, &expires, &attempts, &created, &upd); err != nil {
		return nil, err
	}
	var op state.Operation
	if err := json.Unmarshal(doc, &op); err != nil {
		return nil, err
	}
	op.Status = state.OperationStatus(status)
	op.LeaseOwner = owner
	op.LeaseExpires = time.Time{}
	if expires.Valid {
		op.LeaseExpires = expires.Time.UTC()
	}
	op.Attempts = attempts
	op.CreatedAt, op.UpdatedAt = created.UTC(), upd.UTC()
	return &op, nil
}

func (s *Store) StartOperation(ctx context.Context, st *state.Stack, op *state.Operation) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		if err := saveStack(ctx, tx, st); err != nil {
			return err
		}
		now := time.Now().UTC()
		op.CreatedAt, op.UpdatedAt = now, now
		doc, err := json.Marshal(op)
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `INSERT INTO operations (id, stack, status, doc, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $5) ON CONFLICT (id) DO NOTHING`, op.ID, op.Stack, string(op.Status), string(doc), now)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return state.ErrConflict
		}
		return nil
	})
}

func (s *Store) GetOperation(ctx context.Context, id string) (*state.Operation, error) {
	op, err := scanOp(s.db.QueryRowContext(ctx, `SELECT `+opColumns+` FROM operations WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, state.ErrNotFound
	}
	return op, err
}

func saveOp(ctx context.Context, q execer, op *state.Operation) error {
	doc, err := json.Marshal(op)
	if err != nil {
		return err
	}
	res, err := q.ExecContext(ctx, `UPDATE operations SET status = $3, doc = $4, updated_at = now()
		WHERE id = $1 AND lease_owner = $2`, op.ID, op.LeaseOwner, string(op.Status), string(doc))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		var exists bool
		if err := q.QueryRowContext(ctx, `SELECT true FROM operations WHERE id = $1`, op.ID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
			return state.ErrNotFound
		}
		return state.ErrLeaseLost
	}
	op.UpdatedAt = time.Now().UTC()
	return nil
}

// lockLease locks the operation row and verifies the caller still owns it.
func lockLease(ctx context.Context, tx *sql.Tx, op *state.Operation) error {
	var owner string
	err := tx.QueryRowContext(ctx, `SELECT lease_owner FROM operations WHERE id = $1 FOR UPDATE`, op.ID).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return state.ErrNotFound
	}
	if err != nil {
		return err
	}
	if owner != op.LeaseOwner {
		return state.ErrLeaseLost
	}
	return nil
}

func (s *Store) SaveOperation(ctx context.Context, op *state.Operation) error {
	return saveOp(ctx, s.db, op)
}

func (s *Store) SaveProgress(ctx context.Context, st *state.Stack, op *state.Operation) error {
	// Save a shallow copy so a failed transaction leaves st's version untouched.
	saved := *st
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if err := lockLease(ctx, tx, op); err != nil {
			return err
		}
		if err := saveStack(ctx, tx, &saved); err != nil {
			return err
		}
		return saveOp(ctx, tx, op)
	})
	if err == nil {
		st.Version, st.UpdatedAt, st.CreatedAt = saved.Version, saved.UpdatedAt, saved.CreatedAt
	}
	return err
}

func (s *Store) FinishDelete(ctx context.Context, st *state.Stack, op *state.Operation) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		if err := lockLease(ctx, tx, op); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM stacks WHERE name = $1 AND version = $2`, st.Name, st.Version)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return state.ErrConflict
		}
		return saveOp(ctx, tx, op)
	})
}

func (s *Store) ClaimOperation(ctx context.Context, owner string, lease time.Duration) (*state.Operation, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE operations
		   SET status = 'RUNNING', lease_owner = $1 || '/' || (attempts + 1)::text,
		       lease_expires = now() + ($2 * interval '1 millisecond'),
		       attempts = attempts + 1, updated_at = now()
		 WHERE id = (
		       SELECT id FROM operations
		        WHERE status = 'PENDING' OR (status = 'RUNNING' AND lease_expires < now())
		        ORDER BY created_at
		        FOR UPDATE SKIP LOCKED
		        LIMIT 1)
		RETURNING `+opColumns, owner, lease.Milliseconds())
	op, err := scanOp(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return op, err
}

func (s *Store) RenewLease(ctx context.Context, id, owner string, lease time.Duration) error {
	res, err := s.db.ExecContext(ctx, `UPDATE operations SET lease_expires = now() + ($3 * interval '1 millisecond')
		WHERE id = $1 AND lease_owner = $2 AND status = 'RUNNING'`, id, owner, lease.Milliseconds())
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return state.ErrLeaseLost
	}
	return nil
}

// ------------------------------------------------------------------ events

func (s *Store) AppendEvents(ctx context.Context, evs ...state.Event) error {
	if len(evs) == 0 {
		return nil
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		for i := range evs {
			e := &evs[i]
			if e.Timestamp.IsZero() {
				e.Timestamp = time.Now().UTC()
			}
			doc, err := json.Marshal(e)
			if err != nil {
				return err
			}
			if err := tx.QueryRowContext(ctx, `INSERT INTO events (stack, operation_id, ts, doc) VALUES ($1, $2, $3, $4) RETURNING id`,
				e.Stack, e.OperationID, e.Timestamp, string(doc)).Scan(&e.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ListEvents(ctx context.Context, stack string, afterID int64, limit int) ([]state.Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, doc FROM events WHERE stack = $1 AND id > $2 ORDER BY id LIMIT $3`, stack, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []state.Event
	for rows.Next() {
		var id int64
		var doc []byte
		if err := rows.Scan(&id, &doc); err != nil {
			return nil, err
		}
		var e state.Event
		if err := json.Unmarshal(doc, &e); err != nil {
			return nil, err
		}
		e.ID = id
		out = append(out, e)
	}
	return out, rows.Err()
}
