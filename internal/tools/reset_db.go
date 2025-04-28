package tools

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ResetDB drops and recreates all tables in the database
func ResetDB(ctx context.Context, pool *pgxpool.Pool) error {
	// Drop existing tables if they exist
	_, err := pool.Exec(ctx, `
		DROP TABLE IF EXISTS events;
		DROP TABLE IF EXISTS profiles;
	`)
	if err != nil {
		return fmt.Errorf("failed to drop tables: %w", err)
	}

	// Create profiles table
	_, err = pool.Exec(ctx, `
		CREATE TABLE profiles (
			id SERIAL PRIMARY KEY,
			cookie varchar(4096),
			message_id varchar(1024),
			phone varchar(14)
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create profiles table: %w", err)
	}

	// Create events table
	_, err = pool.Exec(ctx, `
		CREATE TABLE events (
			id SERIAL PRIMARY KEY,
			event_id SMALLINT,
			event_timestamp TIMESTAMP,
			identifiers JSONB,
			processed BOOLEAN DEFAULT FALSE
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create events table: %w", err)
	}

	fmt.Println("Database tables reset successfully")
	return nil
}
