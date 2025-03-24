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
		Expect(profiles).To(ContainElement(existingProfile))
		Expect(profiles).To(ContainElement(db.Profile{
			Cookie:    event.Cookie,
			MessageId: event.MessageId,
			Phone:     event.Phone,
		}))

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

		profiles, err := profileRepo.GetAllProfiles(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(profiles[0]).To(Equal(db.Profile{
			Cookie:    existingProfile.Cookie,
			MessageId: existingProfile.MessageId,
			Phone:     event.Phone,
		}))

		// Verify event was processed
		Eventually(func() []db.EventRecord {
			return eventRepo.ProcessedEvents
		}).Should(HaveLen(1))
		Expect(eventRepo.ProcessedEvents[0]).To(Equal(event))
	})

})
