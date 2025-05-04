package internal

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/tomashoffer/event-stitching/internal/db"
)

type StitchingService struct {
	profileRepo       db.ProfileRepository
	eventRepo         db.EventRepository
	stitchingInterval time.Duration
	numWorkers        int
	batchSize         int
	log               *slog.Logger
}

func NewStitchingService(profileRepo db.ProfileRepository, eventRepo db.EventRepository, stitchingInterval time.Duration, numWorkers, batchSize int) *StitchingService {
	return &StitchingService{
		profileRepo:       profileRepo,
		eventRepo:         eventRepo,
		stitchingInterval: stitchingInterval,
		numWorkers:        numWorkers,
		batchSize:         batchSize,
		log:               slog.Default(),
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
	// Create a helper function for preparing failure results
	fail := func(err error) error {
		return fmt.Errorf("stitch: %w", err)
	}

	// Get a Tx for making transaction requests
	tx, err := s.eventRepo.BeginTx(ctx)
	if err != nil {
		s.log.Error("Failed to begin transaction", "error", fail(err))
		return
	}
	// Defer a rollback in case anything fails
	defer tx.Rollback(ctx)

	// Create a new context with the transaction
	txCtx := context.WithValue(ctx, db.TransactionKey{}, tx)

	// Get unprocessed events within the transaction
	events, err := s.eventRepo.GetUnProcessedEvents(txCtx, s.batchSize)
	if err != nil {
		s.log.Error("Failed to query unstitched events", "error", fail(err))
		return
	}

	for _, event := range events {
		profiles, found, err := s.profileRepo.TryGetProfilesByIdentifiers(txCtx, event.EventIdentifier)
		if err != nil {
			s.log.Warn("Failed to get profile by identifiers, moving on to next event",
				"identifiers", event.EventIdentifier,
				"error", fail(err))
			continue
		}

		if !found {
			s.log.Debug("No profile found by identifiers, creating new profile",
				"identifiers", event.EventIdentifier)
			p := db.Profile{
				Cookie:    event.EventIdentifier.Cookie,
				MessageId: event.EventIdentifier.MessageId,
				Phone:     event.EventIdentifier.Phone,
			}

			_, err = s.profileRepo.InsertProfile(txCtx, p)
			if err != nil {
				s.log.Error("Failed to insert profile", "error", fail(err))
				continue
			}
		} else {
			// At least one profile was found
			if len(profiles) == 1 {
				if err := s.profileRepo.EnrichProfileByIdentifiers(txCtx, profiles[0].Id, event.EventIdentifier); err != nil {
					s.log.Error("Failed to enrich profile", "error", fail(err))
					continue
				}
			} else {
				// Merge profiles if more than one was found
				profileIds := make([]int, len(profiles))
				for i, profile := range profiles {
					profileIds[i] = profile.Id
				}
				if err := s.profileRepo.MergeProfiles(txCtx, profileIds); err != nil {
					s.log.Error("Failed to merge profiles", "error", fail(err))
					continue
				}
			}
		}

		if err := s.eventRepo.MarkEventAsProcessed(txCtx, event); err != nil {
			s.log.Error("Failed to mark event as processed", "error", fail(err))
			continue
		}
		s.log.Debug("Processed event", "event", event)
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		s.log.Error("Failed to commit transaction", "error", fail(err))
		return
	}
}
