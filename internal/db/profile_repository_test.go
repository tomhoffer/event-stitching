package db_test

import (
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/tomashoffer/event-stitching/internal/db"
)

type profileTestContext struct {
	connPool *pgxpool.Pool // TODO: remove
	repo     db.ProfileRepository
}

func setupProfileTest(ctx SpecContext) *profileTestContext {
	var err error
	tc := &profileTestContext{}

	tc.connPool, err = pgxpool.New(ctx, "postgres://myuser:mypassword@localhost:5432/mydatabase")
	Expect(err).NotTo(HaveOccurred())

	tc.repo = db.NewPgProfileRepository(tc.connPool)

	// Clean up the database before each test
	_, err = tc.connPool.Exec(ctx, "TRUNCATE TABLE profiles")
	Expect(err).NotTo(HaveOccurred())

	return tc
}

func (tc *profileTestContext) cleanup() {
	if tc.connPool != nil {
		tc.connPool.Close()
	}
}

var _ = Describe("Profile Repository", func() {
	var tc *profileTestContext

	BeforeEach(func(ctx SpecContext) {
		tc = setupProfileTest(ctx)
	})

	AfterEach(func() {
		tc.cleanup()
	})

	It("should insert and retrieve a profile by each identifier", func(ctx SpecContext) {
		profile := db.Profile{
			Cookie:    "test-cookie",
			MessageId: "test-message",
			Phone:     "123456789",
		}

		err := tc.repo.InsertProfile(ctx, profile)
		Expect(err).NotTo(HaveOccurred())

		// Try to get profile by each identifier
		retrievedProfile, err, found := tc.repo.TryGetProfileByIdentifiers(ctx, db.EventIdentifier{
			Cookie: "test-cookie",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(retrievedProfile).To(Equal(profile))

		retrievedProfile, err, found = tc.repo.TryGetProfileByIdentifiers(ctx, db.EventIdentifier{
			MessageId: "test-message",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(retrievedProfile).To(Equal(profile))

		retrievedProfile, err, found = tc.repo.TryGetProfileByIdentifiers(ctx, db.EventIdentifier{
			Phone: "123456789",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(retrievedProfile).To(Equal(profile))
	})

	It("should update an existing profile", func(ctx SpecContext) {
		originalProfile := db.Profile{
			Cookie:    "original-cookie",
			MessageId: "original-message",
			Phone:     "123456789",
		}

		err := tc.repo.InsertProfile(ctx, originalProfile)
		Expect(err).NotTo(HaveOccurred())

		updatedProfile := db.Profile{
			Cookie:    "updated-cookie",
			MessageId: "updated-message",
			Phone:     "987654321",
		}

		// Get the profile ID (there is only one profile in the table)
		var profileId int
		err = tc.connPool.QueryRow(ctx, "SELECT id FROM profiles LIMIT 1").Scan(&profileId)
		Expect(err).NotTo(HaveOccurred())

		// Update the profile using the fetched ID
		err = tc.repo.UpdateProfileById(ctx, profileId, updatedProfile)
		Expect(err).NotTo(HaveOccurred())

		// Verify the update using GetAllProfiles
		profiles, err := tc.repo.GetAllProfiles(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(profiles).To(HaveLen(1))
		Expect(profiles[0]).To(Equal(updatedProfile))
	})

	It("should return not found for non-existent identifiers", func(ctx SpecContext) {
		_, err, found := tc.repo.TryGetProfileByIdentifiers(ctx, db.EventIdentifier{
			Cookie: "non-existent",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeFalse())
	})

	It("should handle all combinations of identifiers", func(ctx SpecContext) {
		profile := db.Profile{
			Cookie:    "test-cookie",
			MessageId: "test-message",
			Phone:     "123456789",
		}

		err := tc.repo.InsertProfile(ctx, profile)
		Expect(err).NotTo(HaveOccurred())

		// Test all possible combinations of identifiers
		combinations := []db.EventIdentifier{
			{Cookie: "test-cookie", MessageId: "", Phone: ""},
			{Cookie: "", MessageId: "test-message", Phone: ""},
			{Cookie: "", MessageId: "", Phone: "123456789"},
			{Cookie: "test-cookie", MessageId: "test-message", Phone: ""},
			{Cookie: "test-cookie", MessageId: "", Phone: "123456789"},
			{Cookie: "", MessageId: "test-message", Phone: "123456789"},
			{Cookie: "test-cookie", MessageId: "test-message", Phone: "123456789"},
		}

		for _, identifiers := range combinations {
			retrievedProfile, err, found := tc.repo.TryGetProfileByIdentifiers(ctx, identifiers)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(retrievedProfile).To(Equal(profile))
		}
	})

	It("should handle empty identifiers", func(ctx SpecContext) {
		// Test with all empty identifiers
		_, err, found := tc.repo.TryGetProfileByIdentifiers(ctx, db.EventIdentifier{})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeFalse())

		// Test with all empty strings
		_, err, found = tc.repo.TryGetProfileByIdentifiers(ctx, db.EventIdentifier{
			Cookie:    "",
			MessageId: "",
			Phone:     "",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeFalse())
	})

	It("should skip empty identifiers when searching", func(ctx SpecContext) {
		profile := db.Profile{
			Cookie:    "test-cookie",
			MessageId: "", // Empty message_id
			Phone:     "",
		}

		err := tc.repo.InsertProfile(ctx, profile)
		Expect(err).NotTo(HaveOccurred())

		// Try to find profile using empty message_id and non-existent cookie
		_, err, found := tc.repo.TryGetProfileByIdentifiers(ctx, db.EventIdentifier{
			Cookie:    "non-existent-cookie",
			MessageId: "",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeFalse())

		// Try to find profile using empty phone and non-existent cookie
		_, err, found = tc.repo.TryGetProfileByIdentifiers(ctx, db.EventIdentifier{
			Cookie: "non-existent-cookie",
			Phone:  "",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeFalse())
	})

	It("should handle case sensitivity in identifiers", func(ctx SpecContext) {
		profile := db.Profile{
			Cookie:    "Test-Cookie",
			MessageId: "Test-Message",
			Phone:     "123456789",
		}

		err := tc.repo.InsertProfile(ctx, profile)
		Expect(err).NotTo(HaveOccurred())

		// Test with different cases
		combinations := []db.EventIdentifier{
			{Cookie: "test-cookie", MessageId: "", Phone: ""},
			{Cookie: "TEST-COOKIE", MessageId: "", Phone: ""},
			{Cookie: "", MessageId: "test-message", Phone: ""},
			{Cookie: "", MessageId: "TEST-MESSAGE", Phone: ""},
		}

		for _, identifiers := range combinations {
			_, err, found := tc.repo.TryGetProfileByIdentifiers(ctx, identifiers)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse())
		}
	})
})
