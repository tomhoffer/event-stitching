package internal

import (
	"context"
	"fmt"

	"github.com/tomashoffer/event-stitching/internal/db"
)

type EventIngestService struct {
	repo       *db.Repository
	numWorkers int
	Queue      chan db.EventRecord
}

func NewEventIngestService(repo *db.Repository, numWorkers int) *EventIngestService {
	return &EventIngestService{
		repo:       repo,
		numWorkers: numWorkers,
		Queue:      make(chan db.EventRecord, 1000),
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
				fmt.Printf("Failed to insert data: %v\n", err)
				continue
			}
			fmt.Println("Insert successful")
		}
	}
}
