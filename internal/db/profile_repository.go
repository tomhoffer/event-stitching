package db

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ProfileRepository interface {
	TryGetProfilesByIdentifiers(ctx context.Context, identifiers EventIdentifier) ([]Profile, bool, error)
	UpdateProfileById(ctx context.Context, id int, profile Profile) error
	InsertProfile(ctx context.Context, profile Profile) (int, error)
	GetAllProfiles(ctx context.Context) ([]Profile, error)
	EnrichProfileByIdentifiers(ctx context.Context, id int, identifiers EventIdentifier) error
	MergeProfiles(ctx context.Context, profileIds []int) error
}

type PgProfileRepository struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewPgProfileRepository(pool *pgxpool.Pool) *PgProfileRepository {
	return &PgProfileRepository{
		pool: pool,
		log:  slog.Default(),
	}
}

func (r *PgProfileRepository) getProfileByIdentifier(ctx context.Context, identifier string, value string) ([]Profile, error) {
	query := `
		SELECT id, cookie, message_id, phone
		FROM profiles
		WHERE ` + identifier + ` = $1`

	// Get transaction from context if available
	tx, _ := ctx.Value(TransactionKey{}).(pgx.Tx)

	var rows pgx.Rows
	var err error
	if tx != nil {
		rows, err = tx.Query(ctx, query, value)
	} else {
		rows, err = r.pool.Query(ctx, query, value)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query profile by %s: %w", identifier, err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[Profile])
}

func (r *PgProfileRepository) TryGetProfilesByIdentifiers(ctx context.Context, identifiers EventIdentifier) ([]Profile, bool, error) {
	var profiles []Profile
	var err error

	// Try to find profiles by each identifier
	if identifiers.Cookie != "" {
		profiles, err = r.getProfileByIdentifier(ctx, "cookie", identifiers.Cookie)
		if err != nil {
			return nil, false, err
		}
		if len(profiles) > 0 {
			return profiles, true, nil
		}
	}

	if identifiers.MessageId != "" {
		profiles, err = r.getProfileByIdentifier(ctx, "message_id", identifiers.MessageId)
		if err != nil {
			return nil, false, err
		}
		if len(profiles) > 0 {
			return profiles, true, nil
		}
	}

	if identifiers.Phone != "" {
		profiles, err = r.getProfileByIdentifier(ctx, "phone", identifiers.Phone)
		if err != nil {
			return nil, false, err
		}
		if len(profiles) > 0 {
			return profiles, true, nil
		}
	}

	return nil, false, nil
}

func (r *PgProfileRepository) UpdateProfileById(ctx context.Context, id int, profile Profile) error {
	query := `
		UPDATE profiles 
		SET cookie = $1, message_id = $2, phone = $3
		WHERE id = $4`

	// Get transaction from context if available
	tx, _ := ctx.Value(TransactionKey{}).(pgx.Tx)

	var err error
	if tx != nil {
		_, err = tx.Exec(ctx, query, profile.Cookie, profile.MessageId, profile.Phone, id)
	} else {
		_, err = r.pool.Exec(ctx, query, profile.Cookie, profile.MessageId, profile.Phone, id)
	}

	if err != nil {
		return fmt.Errorf("failed to update profile: %w", err)
	}
	return nil
}

func (r *PgProfileRepository) InsertProfile(ctx context.Context, profile Profile) (int, error) {
	query := `
		INSERT INTO profiles (cookie, message_id, phone)
		VALUES ($1, $2, $3)
		RETURNING id`

	// Get transaction from context if available
	tx, _ := ctx.Value(TransactionKey{}).(pgx.Tx)

	var row pgx.Row
	if tx != nil {
		row = tx.QueryRow(ctx, query, profile.Cookie, profile.MessageId, profile.Phone)
	} else {
		row = r.pool.QueryRow(ctx, query, profile.Cookie, profile.MessageId, profile.Phone)
	}

	var id int
	err := row.Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to insert profile: %w", err)
	}
	return id, nil
}

func (r *PgProfileRepository) GetAllProfiles(ctx context.Context) ([]Profile, error) {
	query := `
		SELECT id, cookie, message_id, phone
		FROM profiles`

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
		return nil, fmt.Errorf("failed to query profiles: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[Profile])
}

func (r *PgProfileRepository) EnrichProfileByIdentifiers(ctx context.Context, id int, identifiers EventIdentifier) error {
	query := `
		UPDATE profiles 
		SET 
			cookie = COALESCE(NULLIF($1, ''), cookie),
			message_id = COALESCE(NULLIF($2, ''), message_id),
			phone = COALESCE(NULLIF($3, ''), phone)
		WHERE id = $4`

	// Get transaction from context if available
	tx, _ := ctx.Value(TransactionKey{}).(pgx.Tx)

	var err error
	if tx != nil {
		_, err = tx.Exec(ctx, query, identifiers.Cookie, identifiers.MessageId, identifiers.Phone, id)
	} else {
		_, err = r.pool.Exec(ctx, query, identifiers.Cookie, identifiers.MessageId, identifiers.Phone, id)
	}

	if err != nil {
		return fmt.Errorf("failed to enrich profile: %w", err)
	}
	return nil
}

func (r *PgProfileRepository) MergeProfiles(ctx context.Context, profileIds []int) error {
	if len(profileIds) < 2 {
		return nil
	}

	// Get transaction from context if available
	tx, _ := ctx.Value(TransactionKey{}).(pgx.Tx)
	startedTx := false

	// Start a transaction if one wasn't provided
	var err error
	if tx == nil {
		tx, err = r.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		startedTx = true
		defer tx.Rollback(ctx)
	}

	// Create a new context with the transaction
	txCtx := context.WithValue(ctx, TransactionKey{}, tx)

	// Get all profiles to merge with row locks
	query := `
		SELECT id, cookie, message_id, phone
		FROM profiles
		WHERE id = ANY($1)
		ORDER BY id ASC
		FOR UPDATE`

	rows, err := tx.Query(ctx, query, profileIds)
	if err != nil {
		return fmt.Errorf("failed to query profiles to merge: %w", err)
	}
	defer rows.Close()

	profiles, err := pgx.CollectRows(rows, pgx.RowToStructByName[Profile])
	if err != nil {
		return fmt.Errorf("failed to collect profiles to merge: %w", err)
	}

	if len(profiles) < 2 {
		return nil
	}

	// Find the lowest non-empty values for each field
	var lowestCookie, lowestMessageId, lowestPhone string
	for _, p := range profiles {
		if p.Cookie != "" && (lowestCookie == "" || p.Cookie < lowestCookie) {
			lowestCookie = p.Cookie
		}
		if p.MessageId != "" && (lowestMessageId == "" || p.MessageId < lowestMessageId) {
			lowestMessageId = p.MessageId
		}
		if p.Phone != "" && (lowestPhone == "" || p.Phone < lowestPhone) {
			lowestPhone = p.Phone
		}
	}

	// Create merged profile with the lowest values
	merged := profiles[0]
	merged.Cookie = lowestCookie
	merged.MessageId = lowestMessageId
	merged.Phone = lowestPhone

	// Update the first profile with merged data using the transaction context
	if err := r.UpdateProfileById(txCtx, merged.Id, merged); err != nil {
		return err
	}

	// Delete all other profiles
	deleteQuery := `
		DELETE FROM profiles
		WHERE id = ANY($1) AND id != $2`

	_, err = tx.Exec(ctx, deleteQuery, profileIds, merged.Id)
	if err != nil {
		return fmt.Errorf("failed to delete merged profiles: %w", err)
	}

	// Commit if we started the transaction
	if startedTx {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	return nil
}
