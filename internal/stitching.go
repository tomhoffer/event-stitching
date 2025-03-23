package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/tomashoffer/event-stitching/internal/db"
)

type StitchingService struct {
	repo              *db.Repository
	stitchingInterval time.Duration
	numWorkers        int
	batchSize         int
}

func NewStitchingService(repo *db.Repository, stitchingInterval time.Duration, numWorkers, batchSize int) *StitchingService {
	return &StitchingService{
		repo:              repo,
		stitchingInterval: stitchingInterval,
		numWorkers:        numWorkers,
		batchSize:         batchSize,
	}
}

func (s *StitchingService) Start(ctx context.Context) {
	for i := 0; i < s.numWorkers; i++ {
		go s.stitchWorker(ctx)
	}
}

func (s *StitchingService) stitchWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			s.stitch(ctx)
			time.Sleep(s.stitchingInterval)
		}
	}
}

func (s *StitchingService) stitch(ctx context.Context) {
	fmt.Println("Stitching events")
	events, err := s.repo.GetUnProcessedEvents(ctx, s.batchSize)
	if err != nil {
		fmt.Printf("Failed to query unstitched events: %v\n", err)
		return
	}

	for _, event := range events {
		if err := s.repo.MarkEventAsProcessed(ctx, event); err != nil {
			fmt.Printf("Failed to mark event as stitched: %v\n", err)
			continue
		}
	}
}
