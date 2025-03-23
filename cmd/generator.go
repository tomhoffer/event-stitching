package main

import (
	"context"

	"github.com/tomashoffer/event-stitching/internal"
	"github.com/tomashoffer/event-stitching/internal/db"
)

func GenerateEvents(ctx context.Context, numEvents int, ingestService *internal.EventIngestService) {
	// Send events to queue
	for range numEvents {
		ingestService.Queue <- db.GenerateRandomEvent()
	}
}
