package main

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/tomashoffer/event-stitching/internal"
	"github.com/tomashoffer/event-stitching/internal/db"
)

func TestMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Main Suite")
}

var _ = Describe("Event Record Insertion", func() {
	var connPool *pgxpool.Pool
	var repo *db.Repository
	var ingestService *internal.EventIngestService
	var ingestCtx context.Context
	var cancelIngest context.CancelFunc

	BeforeEach(func(ctx SpecContext) {
		var err error
		connPool, err = pgxpool.New(ctx, "postgres://myuser:mypassword@localhost:5432/mydatabase")
		Expect(err).NotTo(HaveOccurred())

		repo = db.NewRepository(connPool)

		// Clean up the database before each test
		_, err = connPool.Exec(ctx, "TRUNCATE TABLE events")
		Expect(err).NotTo(HaveOccurred())

		// Create a dedicated context for the ingest service
		ingestCtx, cancelIngest = context.WithCancel(context.Background())
		ingestService = internal.NewEventIngestService(repo, 2)
		ingestService.Start(ingestCtx)
	})

	AfterEach(func() {
		if cancelIngest != nil {
			cancelIngest()
		}
		if connPool != nil {
			connPool.Close()
		}
	})

	It("should successfully insert single event record", func(ctx SpecContext) {
		generatedEvent := db.GenerateRandomEvent()

		ingestService.Queue <- generatedEvent

		// Wait for the record count to be 1
		Eventually(func() (int, error) {
			return repo.GetEventsCount(ctx)
		}).WithContext(ctx).Should(Equal(1), "Expected exactly one event to be inserted")

		// Wait for and verify the record contents
		Eventually(func() (db.EventRecord, error) {
			events, err := repo.GetEvents(ctx)
			if err != nil {
				return db.EventRecord{}, err
			}
			if len(events) != 1 {
				return db.EventRecord{}, nil
			}
			return events[0], nil
		}).WithContext(ctx).Should(Equal(generatedEvent), "Expected event details to match")
	})

	It("should successfully insert 10 event records", func(ctx SpecContext) {
		numOfEvents := 10
		insertedEvents := make([]db.EventRecord, numOfEvents)

		for i := 0; i < numOfEvents; i++ {
			insertedEvents[i] = db.GenerateRandomEvent()
			ingestService.Queue <- insertedEvents[i]
		}

		Eventually(func() (int, error) {
			return repo.GetEventsCount(ctx)
		}).WithContext(ctx).Should(Equal(numOfEvents), "Expected all events to be inserted")

		Eventually(func() ([]db.EventRecord, error) {
			return repo.GetEvents(ctx)
		}).WithContext(ctx).Should(ConsistOf(insertedEvents), "Expected all events to match")
	})
})
