package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/KailBug/Kepler/internal/runtime"
	"github.com/KailBug/Kepler/internal/runtime/postgres"
	"github.com/KailBug/Kepler/internal/runtime/storetest"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresStore(t *testing.T) {
	dsn := os.Getenv("KEPLER_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set KEPLER_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	storetest.Run(t, func(t *testing.T) (runtime.Store, func(time.Duration)) {
		now := time.Now()
		s := postgres.New(pool).WithClock(func() time.Time { return now })
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("Migrate: %v", err)
		}
		if _, err := pool.Exec(ctx, `TRUNCATE TABLE events, tasks, sessions, workers CASCADE`); err != nil {
			t.Fatalf("TRUNCATE: %v", err)
		}
		return s, func(d time.Duration) { now = now.Add(d) }
	})
}
