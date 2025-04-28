package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ProfileRepository interface {
	TryGetProfilesByIdentifiers(ctx context.Context, identifier EventIdentifier) (profiles []Profile, found bool, err error)
	UpdateProfileById(ctx context.Context, id int, profile Profile) error
	InsertProfile(ctx context.Context, profile Profile) (id int, err error)
	GetAllProfiles(ctx context.Context) ([]Profile, error)
	EnrichProfileByIdentifiers(ctx context.Context, profileId int, identifier EventIdentifier) error
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

func (r *PgProfileRepository) getProfileByIdentifier(ctx context.Context, identifierName string, identifierVal string) (Profile, bool, error) {
	if identifierVal == "" {
		return Profile{}, false, nil
	}

	identifierName = strings.ToLower(identifierName)
	row := r.pool.QueryRow(ctx, `
			SELECT * FROM profiles WHERE `+identifierName+` = $1`, identifierVal)

	var profile Profile
	var profileId int
	err := row.Scan(&profileId, &profile.Cookie, &profile.MessageId, &profile.Phone)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Profile{}, false, nil
		}
		return Profile{}, false, fmt.Errorf("failed to get profile: %w", err)
	}
	return profile, true, nil
}

func (r *PgProfileRepository) TryGetProfilesByIdentifiers(ctx context.Context, identifier EventIdentifier) (profiles []Profile, found bool, err error) {
	var result []Profile
	profileFound := false
	identifierNames := identifier.GetIdentifierNames()
	for _, identifierName := range identifierNames {
		identifierVal, found := identifier.GetIdentifierValueByName(identifierName)
		if !found {
			continue
		}
		profile, found, err := r.getProfileByIdentifier(ctx, identifierName, identifierVal)
		if err != nil {
			return result, profileFound, fmt.Errorf("failed to get profile by %s: %w", identifierName, err)
		}
		if !found {
			r.log.Debug("Profile not found by identifier, trying next identifier",
				"identifier", identifierName,
				"value", identifierVal)
			continue
		}
		result = append(result, profile)
		profileFound = true
	}

	if len(result) == 0 {
		r.log.Debug("No profile found for any identifier", "identifier", identifier)
	}
	return result, profileFound, nil
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
	rows, err := r.pool.Query(ctx, "SELECT * FROM profiles")
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

// MergeProfiles merges multiple profiles into a single profile by:
// 1. Taking the lexicographically lowest non-empty value for each field
// 2. Updating the profile with the lowest ID with the merged values
// 3. Deleting all other profiles
//
// The method ensures atomicity by performing all operations in a single SQL transaction.
// Profile IDs are sorted to ensure consistent merging behavior.
func (r *PgProfileRepository) MergeProfiles(ctx context.Context, profileIds []int) error {

	// Sort profile IDs to ensure consistent merging
	orderedIds := make([]int, len(profileIds))
	copy(orderedIds, profileIds)
	sort.Ints(orderedIds)

	// Merge profiles and delete others in a single transaction
	_, err := r.pool.Exec(ctx, `
		WITH merged AS (
			SELECT 
				MIN(NULLIF(cookie, '')) as cookie,
				MIN(NULLIF(message_id, '')) as message_id,
				MIN(NULLIF(phone, '')) as phone
			FROM profiles 
			WHERE id = ANY($1)
		),
		updated AS (
			UPDATE profiles 
			SET 
				cookie = COALESCE(merged.cookie, profiles.cookie),
				message_id = COALESCE(merged.message_id, profiles.message_id),
				phone = COALESCE(merged.phone, profiles.phone)
			FROM merged
			WHERE id = $2
		)
		DELETE FROM profiles 
		WHERE id = ANY($1) AND id != $2`,
		orderedIds, orderedIds[0])
	if err != nil {
		return fmt.Errorf("failed to merge and delete profiles: %w", err)
	}

	return nil
}
