package internal

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/tomashoffer/event-stitching/internal/db"
	"github.com/tomashoffer/event-stitching/internal/mocks"
)

func TestStitchingSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Stitching Tests Suite")
}

var _ = Describe("Stitching Service", func() {
	var (
		ctx          context.Context
		profileRepo  *mocks.MockProfileRepository
		eventRepo    *mocks.MockEventRepository
		stitchingSvc *StitchingService
		cancel       context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		profileRepo = mocks.NewMockProfileRepository()
		eventRepo = mocks.NewMockEventRepository()
		stitchingSvc = NewStitchingService(profileRepo, eventRepo, 100*time.Millisecond, 1, 10)
	})

	AfterEach(func() {
		cancel()
	})

	It("should create a new profile when no profile exists", func() {
		event := db.GenerateRandomEvent()
		eventRepo.UnprocessedEvents = append(eventRepo.UnprocessedEvents, event)

		stitchingSvc.Start(ctx)

		// Verify profile was created
		Eventually(func() ([]db.Profile, error) {
			return profileRepo.GetAllProfiles(ctx)
		}).Should(HaveLen(1))

		profiles, err := profileRepo.GetAllProfiles(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(profiles[0].Cookie).To(Equal(event.Cookie))
		Expect(profiles[0].MessageId).To(Equal(event.MessageId))
		Expect(profiles[0].Phone).To(Equal(event.Phone))

		// Verify event was processed
		Eventually(func() []db.EventRecord {
			return eventRepo.ProcessedEvents
		}).Should(HaveLen(1))
		Expect(eventRepo.ProcessedEvents[0]).To(Equal(event))
	})

	It("should create a new profile when no profile matches the identifier", func() {
		// Create an existing profile with different identifiers
		existingProfile := db.Profile{
			Cookie:    "different-cookie",
			MessageId: "different-message",
			Phone:     "987654321",
		}
		profileRepo.InsertProfile(ctx, existingProfile)

		// Create an event with different identifiers
		event := db.EventRecord{
			EventIdentifier: db.EventIdentifier{
				Cookie:    "new-cookie",
				MessageId: "new-message",
				Phone:     "123456789",
			},
		}
		eventRepo.UnprocessedEvents = append(eventRepo.UnprocessedEvents, event)

		stitchingSvc.Start(ctx)

		// Verify a new profile was created
		Eventually(func() ([]db.Profile, error) {
			return profileRepo.GetAllProfiles(ctx)
		}).Should(HaveLen(2))

		profiles, err := profileRepo.GetAllProfiles(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(profiles).To(ContainElement(SatisfyAll(
			HaveField("Cookie", event.Cookie),
			HaveField("MessageId", event.MessageId),
			HaveField("Phone", event.Phone),
		)))

		// Verify event was processed
		Eventually(func() []db.EventRecord {
			return eventRepo.ProcessedEvents
		}).Should(HaveLen(1))
		Expect(eventRepo.ProcessedEvents[0]).To(Equal(event))
	})

	It("should use existing profile when found", func() {
		existingProfile := db.Profile{
			Cookie:    "test-cookie",
			MessageId: "test-message",
			Phone:     "123456789",
		}
		profileRepo.InsertProfile(ctx, existingProfile)

		event := db.EventRecord{
			EventIdentifier: db.EventIdentifier{
				Cookie: "test-cookie",
			},
		}
		eventRepo.UnprocessedEvents = append(eventRepo.UnprocessedEvents, event)

		stitchingSvc.Start(ctx)

		// Verify existing profile was used and no other profiles were created
		Eventually(func() ([]db.Profile, error) {
			return profileRepo.GetAllProfiles(ctx)
		}).Should(HaveLen(1))

		profiles, err := profileRepo.GetAllProfiles(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(profiles[0]).To(Equal(existingProfile))

		// Verify event was processed
		Eventually(func() []db.EventRecord {
			return eventRepo.ProcessedEvents
		}).Should(HaveLen(1))
		Expect(eventRepo.ProcessedEvents[0]).To(Equal(event))
	})

	It("should enrich the profile with the new identifier on event", func() {
		existingProfile := db.Profile{
			Cookie:    "test-cookie",
			MessageId: "test-message",
			Phone:     "",
		}
		_, err := profileRepo.InsertProfile(ctx, existingProfile)
		Expect(err).NotTo(HaveOccurred())

		event := db.EventRecord{
			EventIdentifier: db.EventIdentifier{
				Cookie:    "test-cookie",
				MessageId: "test-message",
				Phone:     "123456789",
			},
		}
		eventRepo.UnprocessedEvents = append(eventRepo.UnprocessedEvents, event)

		stitchingSvc.Start(ctx)

		// Verify existing profile was used and no other profiles were created
		Eventually(func() ([]db.Profile, error) {
			return profileRepo.GetAllProfiles(ctx)
		}).Should(HaveLen(1))
		Eventually(func() ([]db.Profile, error) {
			return profileRepo.GetAllProfiles(ctx)
		}).Should(Equal([]db.Profile{existingProfile}))

		// Verify event was processed
		Eventually(func() []db.EventRecord {
			return eventRepo.ProcessedEvents
		}).Should(HaveLen(1))
		Expect(eventRepo.ProcessedEvents[0]).To(Equal(event))
	})

	It("should trigger profile merge on event with common identifiers", func(ctx SpecContext) {

		profile1 := db.Profile{
			Cookie:    "test-cookie",
			MessageId: "test-message",
			Phone:     "",
		}
		profile1Id, err := profileRepo.InsertProfile(ctx, profile1)
		Expect(err).NotTo(HaveOccurred())

		profile2 := db.Profile{
			Cookie:    "different-cookie",
			MessageId: "different-message", // Common identifier
			Phone:     "123456789",
		}
		profile2Id, err := profileRepo.InsertProfile(ctx, profile2)
		Expect(err).NotTo(HaveOccurred())

		// Create an event that should trigger profile merging
		event := db.EventRecord{
			EventIdentifier: db.EventIdentifier{
				Cookie:    "different-cookie",
				MessageId: "test-message",
				Phone:     "123456789",
			},
		}
		err = eventRepo.InsertEvent(ctx, event)
		Expect(err).NotTo(HaveOccurred())

		// Process the event
		stitchingSvc.Start(ctx)

		// Verify MergeProfiles was called with the correct profile IDs
		Eventually(func() [][]int {
			return profileRepo.MergeCalls
		}, "5s").Should(ContainElement([]int{profile1Id, profile2Id}), "MergeProfiles should be called with correct profile IDs")

		// Verify the event was processed
		Eventually(func() ([]db.EventRecord, error) {
			return eventRepo.GetEvents(ctx)
		}, "5s").Should(HaveLen(1))
	})
})
