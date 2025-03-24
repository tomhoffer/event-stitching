package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ProfileRepository interface {
	TryGetProfileByIdentifiers(ctx context.Context, identifier EventIdentifier) (Profile, error, bool)
	UpdateProfileById(ctx context.Context, id int, profile Profile) error
	InsertProfile(ctx context.Context, profile Profile) error
	GetAllProfiles(ctx context.Context) ([]Profile, error)
}

type PgProfileRepository struct {
	pool *pgxpool.Pool
}

func NewPgProfileRepository(pool *pgxpool.Pool) *PgProfileRepository {
	return &PgProfileRepository{pool: pool}
}

func (r *PgProfileRepository) TryGetProfileByIdentifiers(ctx context.Context, identifier EventIdentifier) (Profile, error, bool) {
	getProfileByIdentifier := func(ctx context.Context, identifierName string, identifierVal string) (Profile, error, bool) {
		if identifierVal == "" {
			return Profile{}, nil, false
		}

		identifierName = strings.ToLower(identifierName)
		row := r.pool.QueryRow(ctx, `
			SELECT cookie, message_id, phone 
			FROM profiles 
			WHERE `+identifierName+` = $1`, identifierVal)

		var profile Profile
		err := row.Scan(&profile.Cookie, &profile.MessageId, &profile.Phone)
		if err != nil {
			if err == pgx.ErrNoRows {
				return Profile{}, nil, false
			}
			return Profile{}, fmt.Errorf("failed to get profile: %w", err), false
		}
		return profile, nil, true
	}

	identifierNames := identifier.GetIdentifierNames()
	for _, identifierName := range identifierNames {
		identifierVal, found := identifier.GetIdentifierValueByName(identifierName)
		if !found {
			continue
		}
		profile, err, found := getProfileByIdentifier(ctx, identifierName, identifierVal)
		if err != nil {
			return Profile{}, fmt.Errorf("failed to get profile by %s: %w", identifierName, err), false
		}
		if !found {
			fmt.Printf("Profile not found by identifier: %v, trying next identifier...\n", identifierName)
			continue
		}
		return profile, nil, true
	}
	fmt.Printf("No profile found for any identifier: %v\n", identifier)
	return Profile{}, nil, false
}

func (r *PgProfileRepository) UpdateProfileById(ctx context.Context, id int, profile Profile) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE profiles SET cookie = $1, message_id = $2, phone = $3 WHERE id = $4`,
		profile.Cookie, profile.MessageId, profile.Phone, id)
	if err != nil {
		return fmt.Errorf("failed to update profile: %w", err)
	}
	return nil
}

func (r *PgProfileRepository) InsertProfile(ctx context.Context, profile Profile) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO profiles (cookie, message_id, phone) VALUES ($1, $2, $3)`,
		profile.Cookie, profile.MessageId, profile.Phone)
	if err != nil {
		return fmt.Errorf("failed to insert profile: %w", err)
	}
	return nil
}

func (r *PgProfileRepository) GetAllProfiles(ctx context.Context) ([]Profile, error) {
	rows, err := r.pool.Query(ctx, "SELECT cookie, message_id, phone FROM profiles")
	if err != nil {
		return nil, fmt.Errorf("failed to query profiles: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[Profile])
}
