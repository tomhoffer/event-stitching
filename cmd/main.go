package main

import (
	"context"
	"fmt"
	"github.com/tomashoffer/event-stitching/internal/tools"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tomashoffer/event-stitching/internal"
	"github.com/tomashoffer/event-stitching/internal/db"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connPool, err := pgxpool.New(ctx, "postgres://myuser:mypassword@localhost:5432/mydatabase")
	defer connPool.Close()
	if err != nil {
		fmt.Printf("Unable to connect: %v\n", err)
		os.Exit(1)
	}

	if err := tools.ResetDB(ctx, connPool); err != nil {
		fmt.Printf("Failed to reset database: %v\n", err)
		os.Exit(1)
	}

	eventRepo := db.NewPgEventRepository(connPool)
	profileRepo := db.NewPgProfileRepository(connPool)

	// Create and start services
	ingestService := internal.NewEventIngestService(eventRepo, 1)
	stitchingService := internal.NewStitchingService(profileRepo, eventRepo, 100*time.Millisecond, 1, 100)

	ingestService.Start(ctx)
	stitchingService.Start(ctx)

	// Generate and ingest test events
	startTime := time.Now()
	GenerateEvents(ctx, 10_000, ingestService)

	// Wait for all events to be processed
	time.Sleep(1 * time.Second)
	for {
		unprocessed, err := eventRepo.GetUnProcessedEvents(ctx, 1)
		if err != nil {
			fmt.Printf("Error getting unprocessed events: %v\n", err)
		}
		if len(unprocessed) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	duration := time.Since(startTime)
	totalEvents, err := eventRepo.GetEventsCount(ctx)
	fmt.Printf("All %d events processed in %v\n", totalEvents, duration)
}
