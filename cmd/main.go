package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tomashoffer/event-stitching/internal"
	"github.com/tomashoffer/event-stitching/internal/db"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	connPool, err := pgxpool.New(ctx, "postgres://myuser:mypassword@localhost:5432/mydatabase")
	if err != nil {
		fmt.Printf("Unable to connect: %v\n", err)
		os.Exit(1)
	}
	defer connPool.Close()

	eventRepo := db.NewPgEventRepository(connPool)
	profileRepo := db.NewPgProfileRepository(connPool)

	// Create and start services
	ingestService := internal.NewEventIngestService(eventRepo, 2)
	stitchingService := internal.NewStitchingService(profileRepo, eventRepo, 1*time.Second, 5, 100)

	ingestService.Start(ctx)
	stitchingService.Start(ctx)

	// Generate and ingest test events
	GenerateEvents(ctx, 1000, ingestService)

	// Wait for context to be done
	<-ctx.Done()
}
