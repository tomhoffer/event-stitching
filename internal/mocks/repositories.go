package mocks

import (
	"context"

	"github.com/tomashoffer/event-stitching/internal/db"
)

type MockProfileRepository struct {
	Profiles map[int]db.Profile
}

func NewMockProfileRepository() *MockProfileRepository {
	return &MockProfileRepository{
		Profiles: make(map[int]db.Profile),
	}
}

func (m *MockProfileRepository) TryGetProfilesByIdentifiers(ctx context.Context, identifier db.EventIdentifier) ([]db.Profile, bool, []int, error) {
	profiles := []db.Profile{}
	ids := []int{}
	found := false

	for id, profile := range m.Profiles {
		if identifier.Cookie != "" && profile.Cookie == identifier.Cookie {
			profiles = append(profiles, profile)
			ids = append(ids, id)
			found = true
		}
		if identifier.MessageId != "" && profile.MessageId == identifier.MessageId {
			profiles = append(profiles, profile)
			ids = append(ids, id)
			found = true
		}
		if identifier.Phone != "" && profile.Phone == identifier.Phone {
			profiles = append(profiles, profile)
			ids = append(ids, id)
			found = true
		}
	}
	return profiles, found, ids, nil
}

func (m *MockProfileRepository) InsertProfile(ctx context.Context, profile db.Profile) (int, error) {
	id := len(m.Profiles)
	m.Profiles[id] = profile
	return id, nil
}

func (m *MockProfileRepository) GetAllProfiles(ctx context.Context) ([]db.Profile, error) {
	profiles := make([]db.Profile, 0, len(m.Profiles))
	for _, profile := range m.Profiles {
		profiles = append(profiles, profile)
	}
	return profiles, nil
}

func (m *MockProfileRepository) UpdateProfileById(ctx context.Context, id int, profile db.Profile) error {
	if _, exists := m.Profiles[id]; exists {
		m.Profiles[id] = profile
		return nil
	}
	return nil
}

func (m *MockProfileRepository) EnrichProfileByIdentifiers(ctx context.Context, profileId int, identifier db.EventIdentifier) error {
	if profile, exists := m.Profiles[profileId]; exists {
		profile.Cookie = identifier.Cookie
		profile.MessageId = identifier.MessageId
		profile.Phone = identifier.Phone
		m.Profiles[profileId] = profile
		return nil
	}
	return nil
}

func (m *MockProfileRepository) MergeProfiles(ctx context.Context, profileIds []int) error {
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
