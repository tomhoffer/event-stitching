package benchmarks

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tomashoffer/event-stitching/internal"
	"github.com/tomashoffer/event-stitching/internal/db"
	"github.com/tomashoffer/event-stitching/internal/tools"
)

func waitForEvents(ctx context.Context, eventRepo db.EventRepository, expectedCount int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		count, err := eventRepo.GetEventsCount(ctx)
		if err != nil {
			return err
		}
		if count == expectedCount {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %d events", expectedCount)
}

func BenchmarkStitchingWithDB(b *testing.B) {
	// Setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	// Insert events directly without using the ingest service
	for i := 0; i < 10_000; i++ {
		event := db.GenerateRandomEvent()
		if err := eventRepo.InsertEvent(ctx, event); err != nil {
			b.Fatalf("Failed to insert event: %v", err)
		}
	}

	// Create stitching service
	stitchingService := internal.NewStitchingService(profileRepo, eventRepo, 1*time.Millisecond, 1, 1)

	// Enable block profiling
	runtime.SetBlockProfileRate(1)

	// Run benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Call Stitch directly
		stitchingService.Stitch(ctx)
	}
	b.StopTimer()
}
