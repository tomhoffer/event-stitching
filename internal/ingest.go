package internal

import (
	"context"
	"log/slog"

	"github.com/tomashoffer/event-stitching/internal/db"
)

type EventIngestService struct {
	repo       db.EventRepository
	numWorkers int
	Queue      chan db.EventRecord
	log        *slog.Logger
}

func NewEventIngestService(repo db.EventRepository, numWorkers int) *EventIngestService {
	return &EventIngestService{
		repo:       repo,
		numWorkers: numWorkers,
		Queue:      make(chan db.EventRecord, 1000),
		log:        slog.Default(),
	}
}

func (s *EventIngestService) Start(ctx context.Context) {
	for range s.numWorkers {
		go s.IngestWorker(ctx)
	}
}

func (s *EventIngestService) IngestWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-s.Queue:
			if !ok {
				return
			}
			if err := s.repo.InsertEvent(ctx, event); err != nil {
				s.log.Error("Failed to insert data", "error", err)
				continue
			}
			s.log.Debug("Insert successful")
		}
	}
}
