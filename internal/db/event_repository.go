package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventRepository interface {
	InsertEvent(ctx context.Context, event EventRecord) error
	GetEvents(ctx context.Context) ([]EventRecord, error)
	GetEventsCount(ctx context.Context) (int, error)
	GetUnProcessedEvents(ctx context.Context, batchSize int) ([]EventRecord, error)
	MarkEventAsProcessed(ctx context.Context, event EventRecord) error
}

type PgEventRepository struct {
	pool *pgxpool.Pool
}

func NewPgEventRepository(pool *pgxpool.Pool) *PgEventRepository {
	return &PgEventRepository{pool: pool}
}

func (r *PgEventRepository) InsertEvent(ctx context.Context, event EventRecord) error {
	_, err := r.pool.Exec(ctx,
		"INSERT INTO events (event_id, event_timestamp, identifiers) VALUES ($1, $2, $3)",
		event.EventId,
		event.EventTimestamp,
		map[string]interface{}{
			"cookie":     event.Cookie,
			"message_id": event.MessageId,
			"phone":      event.Phone,
		})
	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}
	return nil
}

func (r *PgEventRepository) GetEvents(ctx context.Context) ([]EventRecord, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT 
			event_id,
			event_timestamp,
			identifiers->>'cookie' as cookie,
			identifiers->>'message_id' as message_id,
			identifiers->>'phone' as phone
		FROM events`)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[EventRecord])
}

func (r *PgEventRepository) GetEventsCount(ctx context.Context) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM events").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get events count: %w", err)
	}
	return count, nil
}

func (r *PgEventRepository) GetUnProcessedEvents(ctx context.Context, batchSize int) ([]EventRecord, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT 
			event_id,
			event_timestamp,
			identifiers->>'cookie' as cookie,
			identifiers->>'message_id' as message_id,
			identifiers->>'phone' as phone
		FROM events 
		WHERE processed = false 
		ORDER BY event_timestamp ASC 
		LIMIT $1 
		FOR UPDATE`, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query unstitched events: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[EventRecord])
}

func (r *PgEventRepository) MarkEventAsProcessed(ctx context.Context, event EventRecord) error {
	_, err := r.pool.Exec(ctx,
		"UPDATE events SET processed = true WHERE event_id = $1 AND event_timestamp = $2",
		event.EventId, event.EventTimestamp)
	if err != nil {
		return fmt.Errorf("failed to mark event as processed: %w", err)
	}
	return nil
}

func (r *PgEventRepository) GetEventsByTimeRange(ctx context.Context, start, end time.Time) ([]EventRecord, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT 
			event_id,
			event_timestamp,
			identifiers->>'cookie' as cookie,
			identifiers->>'message_id' as message_id,
			identifiers->>'phone' as phone
		FROM events 
		WHERE event_timestamp BETWEEN $1 AND $2
		ORDER BY event_timestamp ASC`, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query events by time range: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[EventRecord])
}
