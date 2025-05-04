package benchmarks

import (
	"context"
	"github.com/tomashoffer/event-stitching/internal/tools"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tomashoffer/event-stitching/internal"
	"github.com/tomashoffer/event-stitching/internal/db"
)

func BenchmarkStitchingWithDB(b *testing.B) {
	// Setup
	ctx := context.Background()
	connPool, err := pgxpool.New(ctx, "postgres://myuser:mypassword@localhost:5432/mydatabase")
	if err != nil {
		b.Fatalf("Failed to connect to database: %v", err)
	}
	defer connPool.Close()

	// Reset database
	if err := tools.ResetDB(ctx, connPool); err != nil {
		b.Fatalf("Failed to reset database: %v", err)
	}

	// Create repositories
	profileRepo := db.NewPgProfileRepository(connPool)
	eventRepo := db.NewPgEventRepository(connPool)

	// Create service
	svc := internal.NewStitchingService(profileRepo, eventRepo, 1*time.Millisecond, 1, 1)

	// Enable block profiling
	runtime.SetBlockProfileRate(1)

	// Run benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		svc.Stitch(ctx)
	}
}
