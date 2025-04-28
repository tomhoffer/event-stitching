package db_test

import (
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
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

		_, err := tc.repo.InsertProfile(ctx, profile)
		Expect(err).NotTo(HaveOccurred())

		// Try to get profile by each identifier
		retrievedProfiles, found, err := tc.repo.TryGetProfilesByIdentifiers(ctx, db.EventIdentifier{
			Cookie: "test-cookie",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(retrievedProfiles[0]).To(Equal(profile))

		retrievedProfiles, found, err = tc.repo.TryGetProfilesByIdentifiers(ctx, db.EventIdentifier{
			MessageId: "test-message",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(retrievedProfiles[0]).To(Equal(profile))

		retrievedProfiles, found, err = tc.repo.TryGetProfilesByIdentifiers(ctx, db.EventIdentifier{
			Phone: "123456789",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(retrievedProfiles[0]).To(Equal(profile))
	})

	DescribeTable("should enrich profile data",
		func(ctx SpecContext, originalProfile db.Profile, identifiers db.EventIdentifier, expectedProfile db.Profile) {
			profileId, err := tc.repo.InsertProfile(ctx, originalProfile)
			Expect(err).NotTo(HaveOccurred())

			err = tc.repo.EnrichProfileByIdentifiers(ctx, profileId, identifiers)
			Expect(err).NotTo(HaveOccurred())

			profiles, err := tc.repo.GetAllProfiles(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(profiles).To(HaveLen(1))
			Expect(profiles[0]).To(MatchFields(IgnoreExtras, Fields{
				"Cookie":    Equal(expectedProfile.Cookie),
				"MessageId": Equal(expectedProfile.MessageId),
				"Phone":     Equal(expectedProfile.Phone),
			}))
		},
		Entry("update all fields",
			db.Profile{
				Cookie:    "original-cookie",
				MessageId: "original-message",
				Phone:     "123456789",
			},
			db.EventIdentifier{
				Cookie:    "updated-cookie",
				MessageId: "updated-message",
				Phone:     "987654321",
			},
			db.Profile{
				Cookie:    "updated-cookie",
				MessageId: "updated-message",
				Phone:     "987654321",
			},
		),
		Entry("update single field",
			db.Profile{
				Cookie:    "original-cookie",
				MessageId: "original-message",
				Phone:     "123456789",
			},
			db.EventIdentifier{
				Cookie:    "original-cookie",
				MessageId: "updated-message",
				Phone:     "123456789",
			},
			db.Profile{
				Cookie:    "original-cookie",
				MessageId: "updated-message",
				Phone:     "123456789",
			},
		),
		Entry("enrich with new identifier",
			db.Profile{
				Cookie:    "test-cookie",
				MessageId: "test-message",
				Phone:     "",
			},
			db.EventIdentifier{
				Cookie:    "test-cookie",
				MessageId: "test-message",
				Phone:     "123456789",
			},
			db.Profile{
				Cookie:    "test-cookie",
				MessageId: "test-message",
				Phone:     "123456789",
			},
		),
		Entry("not enrich with empty identifier",
			db.Profile{
				Cookie:    "test-cookie",
				MessageId: "test-message",
				Phone:     "123456789",
			},
			db.EventIdentifier{
				Cookie:    "test-cookie",
				MessageId: "",
				Phone:     "",
			},
			db.Profile{
				Cookie:    "test-cookie",
				MessageId: "test-message",
				Phone:     "123456789",
			},
		),
		Entry("not enrich with missing identifier",
			db.Profile{
				Cookie:    "test-cookie",
				MessageId: "test-message",
				Phone:     "123456789",
			},
			db.EventIdentifier{
				Cookie: "test-cookie",
			},
			db.Profile{
				Cookie:    "test-cookie",
				MessageId: "test-message",
				Phone:     "123456789",
			},
		),
	)

	It("should return not found for non-existent identifiers", func(ctx SpecContext) {
		_, found, err := tc.repo.TryGetProfilesByIdentifiers(ctx, db.EventIdentifier{
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

		_, err := tc.repo.InsertProfile(ctx, profile)
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
			retrievedProfiles, found, err := tc.repo.TryGetProfilesByIdentifiers(ctx, identifiers)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(retrievedProfiles[0]).To(Equal(profile))
		}
	})

	It("should handle empty identifiers", func(ctx SpecContext) {
		// Test with all empty identifiers
		_, found, err := tc.repo.TryGetProfilesByIdentifiers(ctx, db.EventIdentifier{})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeFalse())

		// Test with all empty strings
		_, found, err = tc.repo.TryGetProfilesByIdentifiers(ctx, db.EventIdentifier{
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

		_, err := tc.repo.InsertProfile(ctx, profile)
		Expect(err).NotTo(HaveOccurred())

		// Try to find profile using empty message_id and non-existent cookie
		_, found, err := tc.repo.TryGetProfilesByIdentifiers(ctx, db.EventIdentifier{
			Cookie:    "non-existent-cookie",
			MessageId: "",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeFalse())

		// Try to find profile using empty phone and non-existent cookie
		_, found, err = tc.repo.TryGetProfilesByIdentifiers(ctx, db.EventIdentifier{
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

		_, err := tc.repo.InsertProfile(ctx, profile)
		Expect(err).NotTo(HaveOccurred())

		// Test with different cases
		combinations := []db.EventIdentifier{
			{Cookie: "test-cookie", MessageId: "", Phone: ""},
			{Cookie: "TEST-COOKIE", MessageId: "", Phone: ""},
			{Cookie: "", MessageId: "test-message", Phone: ""},
			{Cookie: "", MessageId: "TEST-MESSAGE", Phone: ""},
		}

		for _, identifiers := range combinations {
			_, found, err := tc.repo.TryGetProfilesByIdentifiers(ctx, identifiers)
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse())
		}
	})

	Describe("Profile Merging", func() {
		It("should merge profiles and keep the lowest values", func(ctx SpecContext) {
			// Create first profile
			profile1 := db.Profile{
				Cookie:    "test-cookie",
				MessageId: "test-message",
				Phone:     "123456789",
			}
			profile1Id, err := tc.repo.InsertProfile(ctx, profile1)
			Expect(err).NotTo(HaveOccurred())

			// Create second profile with different values
			profile2 := db.Profile{
				Cookie:    "a-cookie",  // Lower value
				MessageId: "a-message", // Lower value
				Phone:     "987654321",
			}
			profile2Id, err := tc.repo.InsertProfile(ctx, profile2)
			Expect(err).NotTo(HaveOccurred())

			// Merge profiles
			err = tc.repo.MergeProfiles(ctx, []int{profile1Id, profile2Id})
			Expect(err).NotTo(HaveOccurred())

			// Verify only one profile remains
			profiles, err := tc.repo.GetAllProfiles(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(profiles).To(HaveLen(1))

			// Verify merged profile has the lowest values
			Expect(profiles[0]).To(Equal(db.Profile{
				Id:        profile1Id,
				Cookie:    "a-cookie",
				MessageId: "a-message",
				Phone:     "123456789",
			}))
		})

		It("should preserve non-empty values when merging", func(ctx SpecContext) {
			// Create first profile with some empty values
			profile1 := db.Profile{
				Cookie:    "test-cookie",
				MessageId: "",
				Phone:     "123456789",
			}
			profile1Id, err := tc.repo.InsertProfile(ctx, profile1)
			Expect(err).NotTo(HaveOccurred())

			// Create second profile with different values
			profile2 := db.Profile{
				Cookie:    "",
				MessageId: "test-message",
				Phone:     "",
			}
			profile2Id, err := tc.repo.InsertProfile(ctx, profile2)
			Expect(err).NotTo(HaveOccurred())

			// Merge profiles
			err = tc.repo.MergeProfiles(ctx, []int{profile1Id, profile2Id})
			Expect(err).NotTo(HaveOccurred())

			// Verify merged profile preserves non-empty values
			profiles, err := tc.repo.GetAllProfiles(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(profiles[0]).To(Equal(db.Profile{
				Id:        profile1Id,
				Cookie:    "test-cookie",
				MessageId: "test-message",
				Phone:     "123456789",
			}))
		})

		It("should handle merging multiple profiles", func(ctx SpecContext) {
			// Create three profiles with different values
			profiles := []db.Profile{
				{Cookie: "c-cookie", MessageId: "c-message", Phone: "123456789"},
				{Cookie: "b-cookie", MessageId: "b-message", Phone: "987654321"},
				{Cookie: "a-cookie", MessageId: "a-message", Phone: "456789123"},
			}

			profileIds := make([]int, len(profiles))
			for i, profile := range profiles {
				id, err := tc.repo.InsertProfile(ctx, profile)
				Expect(err).NotTo(HaveOccurred())
				profileIds[i] = id
			}

			// Merge profiles
			err := tc.repo.MergeProfiles(ctx, profileIds)
			Expect(err).NotTo(HaveOccurred())

			// Verify merged profile has the lowest values
			slices.Sort(profileIds)
			lowestId := profileIds[0]
			mergedProfiles, err := tc.repo.GetAllProfiles(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(mergedProfiles[0]).To(Equal(db.Profile{
				Id:        lowestId,
				Cookie:    "a-cookie",
				MessageId: "a-message",
				Phone:     "123456789",
			}))
		})
	})
})
