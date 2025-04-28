package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tomashoffer/event-stitching/internal"
	"github.com/tomashoffer/event-stitching/internal/db"
	"github.com/tomashoffer/event-stitching/internal/logger"
	"github.com/tomashoffer/event-stitching/internal/tools"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := logger.NewLogger(slog.LevelInfo)

	connPool, err := pgxpool.New(ctx, "postgres://myuser:mypassword@localhost:5432/mydatabase")
	defer connPool.Close()
	if err != nil {
		log.Error("Unable to connect", "error", err)
		os.Exit(1)
	}

	if err := tools.ResetDB(ctx, connPool); err != nil {
		log.Error("Failed to reset database", "error", err)
		os.Exit(1)
	}

	eventRepo := db.NewPgEventRepository(connPool)
	profileRepo := db.NewPgProfileRepository(connPool)

	// Create and start services
	ingestService := internal.NewEventIngestService(eventRepo, 1)
	stitchingService := internal.NewStitchingService(profileRepo, eventRepo, 100*time.Millisecond, 5, 100)

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
			log.Error("Error getting unprocessed events", "error", err)
		}
		if len(unprocessed) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	duration := time.Since(startTime)
	totalEvents, err := eventRepo.GetEventsCount(ctx)
	log.Info("All events processed", "count", totalEvents, "duration", duration)
}
