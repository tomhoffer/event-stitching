package mocks

import (
	"context"
	"sort"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jackc/pgx/v5"
	"github.com/tomashoffer/event-stitching/internal/db"
)

type MockPgxTx struct{}

func (m *MockPgxTx) Begin(ctx context.Context) (pgx.Tx, error) { return nil, nil }
func (m *MockPgxTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (m *MockPgxTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults { return nil }
func (m *MockPgxTx) LargeObjects() pgx.LargeObjects                               { return pgx.LargeObjects{} }
func (m *MockPgxTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (m *MockPgxTx) Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error) {
	return pgconn.CommandTag{}, nil
}
func (m *MockPgxTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, nil
}
func (m *MockPgxTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row { return nil }
func (m *MockPgxTx) Conn() *pgx.Conn                                               { return nil }
func (m *MockPgxTx) Commit(ctx context.Context) error                              { return nil }
func (m *MockPgxTx) Rollback(ctx context.Context) error                            { return nil }

type MockProfileRepository struct {
	Profiles   map[int]db.Profile
	MergeCalls [][]int
}

func NewMockProfileRepository() *MockProfileRepository {
	return &MockProfileRepository{
		Profiles:   make(map[int]db.Profile),
		MergeCalls: make([][]int, 0),
	}
}

func (m *MockProfileRepository) TryGetProfilesByIdentifiers(ctx context.Context, identifier db.EventIdentifier) ([]db.Profile, bool, error) {
	var profiles []db.Profile
	found := false

	// Create a slice of profile IDs to ensure deterministic order
	profileIds := make([]int, 0, len(m.Profiles))
	for id := range m.Profiles {
		profileIds = append(profileIds, id)
	}
	sort.Ints(profileIds)

	// Iterate through profiles in sorted order
	for _, id := range profileIds {
		profile := m.Profiles[id]
		if identifier.Cookie != "" && profile.Cookie == identifier.Cookie {
			profiles = append(profiles, profile)
			found = true
			continue
		}
		if identifier.MessageId != "" && profile.MessageId == identifier.MessageId {
			profiles = append(profiles, profile)
			found = true
			continue
		}
		if identifier.Phone != "" && profile.Phone == identifier.Phone {
			profiles = append(profiles, profile)
			found = true
			continue
		}
	}
	return profiles, found, nil
}

func (m *MockProfileRepository) InsertProfile(ctx context.Context, profile db.Profile) (int, error) {
	id := len(m.Profiles)
	profile.Id = id
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
		if identifier.Cookie != "" {
			profile.Cookie = identifier.Cookie
		}
		if identifier.MessageId != "" {
			profile.MessageId = identifier.MessageId
		}
		if identifier.Phone != "" {
			profile.Phone = identifier.Phone
		}
		m.Profiles[profileId] = profile
		return nil
	}
	return nil
}

func (m *MockProfileRepository) MergeProfiles(ctx context.Context, profileIds []int) error {
	m.MergeCalls = append(m.MergeCalls, profileIds)
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

func (m *MockEventRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return &MockPgxTx{}, nil
}
