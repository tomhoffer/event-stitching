package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TransactionKey is a type-safe key for storing transactions in context
type TransactionKey struct{}

type EventRepository interface {
	GetUnProcessedEvents(ctx context.Context, batchSize int) ([]EventRecord, error)
	MarkEventAsProcessed(ctx context.Context, event EventRecord) error
	GetEvents(ctx context.Context) ([]EventRecord, error)
	GetEventsCount(ctx context.Context) (int, error)
	InsertEvent(ctx context.Context, event EventRecord) error
	BeginTx(ctx context.Context) (pgx.Tx, error)
}

type PgEventRepository struct {
	pool *pgxpool.Pool
}

func NewPgEventRepository(pool *pgxpool.Pool) *PgEventRepository {
	return &PgEventRepository{pool: pool}
}

func (r *PgEventRepository) InsertEvent(ctx context.Context, event EventRecord) error {
	query := "INSERT INTO events (event_id, event_timestamp, identifiers) VALUES ($1, $2, $3)"
	args := []interface{}{
		event.EventId,
		event.EventTimestamp,
		map[string]interface{}{
			"cookie":     event.Cookie,
			"message_id": event.MessageId,
			"phone":      event.Phone,
		},
	}

	// Get transaction from context if available
	tx, _ := ctx.Value(TransactionKey{}).(pgx.Tx)

	var err error
	if tx != nil {
		_, err = tx.Exec(ctx, query, args...)
	} else {
		_, err = r.pool.Exec(ctx, query, args...)
	}

	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}
	return nil
}

func (r *PgEventRepository) GetEvents(ctx context.Context) ([]EventRecord, error) {
	query := `
		SELECT 
			event_id,
			event_timestamp,
			identifiers->>'cookie' as cookie,
			identifiers->>'message_id' as message_id,
			identifiers->>'phone' as phone
		FROM events`

	// Get transaction from context if available
	tx, _ := ctx.Value(TransactionKey{}).(pgx.Tx)

	var rows pgx.Rows
	var err error
	if tx != nil {
		rows, err = tx.Query(ctx, query)
	} else {
		rows, err = r.pool.Query(ctx, query)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[EventRecord])
}

func (r *PgEventRepository) GetEventsCount(ctx context.Context) (int, error) {
	query := "SELECT COUNT(*) FROM events"

	// Get transaction from context if available
	tx, _ := ctx.Value(TransactionKey{}).(pgx.Tx)

	var row pgx.Row
	if tx != nil {
		row = tx.QueryRow(ctx, query)
	} else {
		row = r.pool.QueryRow(ctx, query)
	}

	var count int
	err := row.Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get events count: %w", err)
	}
	return count, nil
}

func (r *PgEventRepository) GetUnProcessedEvents(ctx context.Context, batchSize int) ([]EventRecord, error) {
	query := `
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
		FOR UPDATE`

	// Get transaction from context if available
	tx, _ := ctx.Value(TransactionKey{}).(pgx.Tx)

	var rows pgx.Rows
	var err error
	if tx != nil {
		rows, err = tx.Query(ctx, query, batchSize)
	} else {
		rows, err = r.pool.Query(ctx, query, batchSize)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query unstitched events: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[EventRecord])
}

func (r *PgEventRepository) MarkEventAsProcessed(ctx context.Context, event EventRecord) error {
	query := "UPDATE events SET processed = true WHERE event_id = $1 AND event_timestamp = $2"

	// Get transaction from context if available
	tx, _ := ctx.Value(TransactionKey{}).(pgx.Tx)

	var err error
	if tx != nil {
		_, err = tx.Exec(ctx, query, event.EventId, event.EventTimestamp)
	} else {
		_, err = r.pool.Exec(ctx, query, event.EventId, event.EventTimestamp)
	}

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

func (r *PgEventRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}
