package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ProfileRepository interface {
	TryGetProfileByIdentifiers(ctx context.Context, identifier EventIdentifier) (profile Profile, found bool, id int, err error)
	UpdateProfileById(ctx context.Context, id int, profile Profile) error
	InsertProfile(ctx context.Context, profile Profile) (id int, err error)
	GetAllProfiles(ctx context.Context) ([]Profile, error)
	EnrichProfileByIdentifiers(ctx context.Context, profileId int, identifier EventIdentifier) error
}

type PgProfileRepository struct {
	pool *pgxpool.Pool
}

func NewPgProfileRepository(pool *pgxpool.Pool) *PgProfileRepository {
	return &PgProfileRepository{pool: pool}
}

func (r *PgProfileRepository) TryGetProfileByIdentifiers(ctx context.Context, identifier EventIdentifier) (profile Profile, found bool, id int, err error) {
	getProfileByIdentifier := func(ctx context.Context, identifierName string, identifierVal string) (Profile, bool, int, error) {
		if identifierVal == "" {
			return Profile{}, false, 0, nil
		}

		identifierName = strings.ToLower(identifierName)
		row := r.pool.QueryRow(ctx, `
			SELECT * FROM profiles WHERE `+identifierName+` = $1`, identifierVal)

		var profile Profile
		var profileId int
		err := row.Scan(&profileId, &profile.Cookie, &profile.MessageId, &profile.Phone)
		if err != nil {
			if err == pgx.ErrNoRows {
				return Profile{}, false, 0, nil
			}
			return Profile{}, false, 0, fmt.Errorf("failed to get profile: %w", err)
		}
		return profile, true, profileId, nil
	}

	identifierNames := identifier.GetIdentifierNames()
	for _, identifierName := range identifierNames {
		identifierVal, found := identifier.GetIdentifierValueByName(identifierName)
		if !found {
			continue
		}
		profile, found, profileId, err := getProfileByIdentifier(ctx, identifierName, identifierVal)
		if err != nil {
			return Profile{}, false, 0, fmt.Errorf("failed to get profile by %s: %w", identifierName, err)
		}
		if !found {
			fmt.Printf("Profile not found by identifier: %v, trying next identifier...\n", identifierName)
			continue
		}
		return profile, true, profileId, nil
	}
	fmt.Printf("No profile found for any identifier: %v\n", identifier)
	return Profile{}, false, 0, nil
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

func (r *PgProfileRepository) InsertProfile(ctx context.Context, profile Profile) (profileId int, err error) {
	var id int
	err = r.pool.QueryRow(ctx, `
		INSERT INTO profiles (cookie, message_id, phone) VALUES ($1, $2, $3) RETURNING id`,
		profile.Cookie, profile.MessageId, profile.Phone).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to insert profile: %w", err)
	}
	return id, nil
}

func (r *PgProfileRepository) GetAllProfiles(ctx context.Context) ([]Profile, error) {
	rows, err := r.pool.Query(ctx, "SELECT cookie, message_id, phone FROM profiles")
	if err != nil {
		return nil, fmt.Errorf("failed to query profiles: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[Profile])
}

func (r *PgProfileRepository) EnrichProfileByIdentifiers(ctx context.Context, profileId int, identifier EventIdentifier) error {
	identifierNames := identifier.GetIdentifierNames()
	for _, identifierName := range identifierNames {
		identifierVal, found := identifier.GetIdentifierValueByName(identifierName)
		if !found || identifierVal == "" {
			continue
		} // TODO group all updates into 1 query
		_, err := r.pool.Exec(ctx, `UPDATE profiles SET `+identifierName+` = $1 WHERE id = $2`, identifierVal, profileId)
		if err != nil {
			return fmt.Errorf("failed to add identifier to profile: %w", err)
		}
	}
	return nil
}
