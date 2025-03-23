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
	events, err := s.repo.GetUnProcessedEvents(ctx, s.batchSize)
	if err != nil {
		fmt.Printf("Failed to query unstitched events: %v\n", err)
		return
	}

	for _, event := range events {
		profile, err, found := s.repo.TryGetProfileByIdentifiers(ctx, event.EventIdentifier)
		if err != nil {
			fmt.Printf("Failed to get profile by identifiers, moving on to next event... %v: %v\n", event.EventIdentifier, err)
			continue
		}

		if !found {
			fmt.Printf("No profile found by identifiers %v, creating new profile...\n", event.EventIdentifier)
			profile = db.Profile{
				Cookie:    event.EventIdentifier.Cookie,
				MessageId: event.EventIdentifier.MessageId,
				Phone:     event.EventIdentifier.Phone,
			}

			err = s.repo.InsertProfile(ctx, profile)
			if err != nil {
				fmt.Printf("Failed to insert profile: %v\n", err)
				continue
			}
		}
		fmt.Printf("Stitched event: %v\n", profile)
		if err := s.repo.MarkEventAsProcessed(ctx, event); err != nil {
			fmt.Printf("Failed to mark event as stitched: %v\n", err)
			continue
		}
	}
}
