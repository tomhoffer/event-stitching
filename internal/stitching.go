package internal

import (
	"context"
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
	events, err := s.eventRepo.GetUnProcessedEvents(ctx, s.batchSize)
	if err != nil {
		s.log.Error("Failed to query unstitched events", "error", err)
		return
	}

	for _, event := range events {
		profiles, found, err := s.profileRepo.TryGetProfilesByIdentifiers(ctx, event.EventIdentifier)
		if err != nil {
			s.log.Warn("Failed to get profile by identifiers, moving on to next event",
				"identifiers", event.EventIdentifier,
				"error", err)
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

			_, err = s.profileRepo.InsertProfile(ctx, p)
			if err != nil {
				s.log.Error("Failed to insert profile", "error", err)
				continue
			}
		} else {
			// At least one profile was found
			if len(profiles) == 1 {
				if err := s.profileRepo.EnrichProfileByIdentifiers(ctx, profiles[0].Id, event.EventIdentifier); err != nil {
					s.log.Error("Failed to enrich profile", "error", err)
					continue
				}
			} else {
				// Merge profiles if more than one was found
				profileIds := make([]int, len(profiles))
				for i, profile := range profiles {
					profileIds[i] = profile.Id
				}
				s.profileRepo.MergeProfiles(ctx, profileIds)
			}
		}

		if err := s.eventRepo.MarkEventAsProcessed(ctx, event); err != nil {
			s.log.Error("Failed to mark event as processed", "error", err)
			continue
		}
		s.log.Debug("Processed event", "event", event)
	}
}
