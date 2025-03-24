package mocks

import (
	"context"

	"github.com/tomashoffer/event-stitching/internal/db"
)

type MockProfileRepository struct {
	Profiles []db.Profile
}

func NewMockProfileRepository() *MockProfileRepository {
	return &MockProfileRepository{
		Profiles: make([]db.Profile, 0),
	}
}

func (m *MockProfileRepository) TryGetProfileByIdentifiers(ctx context.Context, identifier db.EventIdentifier) (db.Profile, error, bool) {
	for _, profile := range m.Profiles {
		if identifier.Cookie != "" && profile.Cookie == identifier.Cookie {
			return profile, nil, true
		}
		if identifier.MessageId != "" && profile.MessageId == identifier.MessageId {
			return profile, nil, true
		}
		if identifier.Phone != "" && profile.Phone == identifier.Phone {
			return profile, nil, true
		}
	}
	return db.Profile{}, nil, false
}

func (m *MockProfileRepository) InsertProfile(ctx context.Context, profile db.Profile) error {
	m.Profiles = append(m.Profiles, profile)
	return nil
}

func (m *MockProfileRepository) GetAllProfiles(ctx context.Context) ([]db.Profile, error) {
	return m.Profiles, nil
}

func (m *MockProfileRepository) UpdateProfileById(ctx context.Context, id int, profile db.Profile) error {
	if id >= 0 && id < len(m.Profiles) {
		m.Profiles[id] = profile
		return nil
	}
	return nil
}

type MockEventRepository struct {
	UnprocessedEvents []db.EventRecord
	ProcessedEvents   []db.EventRecord
}

func NewMockEventRepository() *MockEventRepository {
	return &MockEventRepository{
		UnprocessedEvents: make([]db.EventRecord, 0),
		ProcessedEvents:   make([]db.EventRecord, 0),
	}
}

func (m *MockEventRepository) GetUnProcessedEvents(ctx context.Context, batchSize int) ([]db.EventRecord, error) {
	if len(m.UnprocessedEvents) > batchSize {
		events := m.UnprocessedEvents[:batchSize]
		m.UnprocessedEvents = m.UnprocessedEvents[batchSize:]
		return events, nil
	}
	events := m.UnprocessedEvents
	m.UnprocessedEvents = nil
	return events, nil
}

func (m *MockEventRepository) MarkEventAsProcessed(ctx context.Context, event db.EventRecord) error {
	m.ProcessedEvents = append(m.ProcessedEvents, event)
	return nil
}

func (m *MockEventRepository) GetEvents(ctx context.Context) ([]db.EventRecord, error) {
	return append(m.ProcessedEvents, m.UnprocessedEvents...), nil
}

func (m *MockEventRepository) GetEventsCount(ctx context.Context) (int, error) {
	return len(m.ProcessedEvents) + len(m.UnprocessedEvents), nil
}

func (m *MockEventRepository) InsertEvent(ctx context.Context, event db.EventRecord) error {
	m.UnprocessedEvents = append(m.UnprocessedEvents, event)
	return nil
}
