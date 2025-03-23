package db_test

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

type testContext struct {
	connPool      *pgxpool.Pool
	repo          *db.Repository
	ingestService *internal.EventIngestService
	ingestCtx     context.Context
	cancelIngest  context.CancelFunc
}

func setupTest(ctx SpecContext, numWorkers int) *testContext {
	var err error
	tc := &testContext{}

	tc.connPool, err = pgxpool.New(ctx, "postgres://myuser:mypassword@localhost:5432/mydatabase")
	Expect(err).NotTo(HaveOccurred())

	tc.repo = db.NewRepository(tc.connPool)

	// Clean up the database before each test
	_, err = tc.connPool.Exec(ctx, "TRUNCATE TABLE events")
	Expect(err).NotTo(HaveOccurred())

	// Create a dedicated context for the ingest service
	tc.ingestCtx, tc.cancelIngest = context.WithCancel(context.Background())
	tc.ingestService = internal.NewEventIngestService(tc.repo, numWorkers)
	tc.ingestService.Start(tc.ingestCtx)

	return tc
}

func (tc *testContext) cleanup() {
	if tc.cancelIngest != nil {
		tc.cancelIngest()
	}
	if tc.connPool != nil {
		tc.connPool.Close()
	}
}

func (tc *testContext) testSingleEvent(ctx SpecContext) {
	generatedEvent := db.GenerateRandomEvent()

	tc.ingestService.Queue <- generatedEvent

	// Wait for the record count to be 1
	Eventually(func() (int, error) {
		return tc.repo.GetEventsCount(ctx)
	}).WithContext(ctx).Should(Equal(1), "Expected exactly one event to be inserted")

	// Wait for and verify the record contents
	Eventually(func() (db.EventRecord, error) {
		events, err := tc.repo.GetEvents(ctx)
		if err != nil {
			return db.EventRecord{}, err
		}
		if len(events) != 1 {
			return db.EventRecord{}, nil
		}
		return events[0], nil
	}).WithContext(ctx).Should(Equal(generatedEvent), "Expected event details to match")
}

func (tc *testContext) testMultipleEvents(ctx SpecContext) {
	numOfEvents := 10
	insertedEvents := make([]db.EventRecord, numOfEvents)

	for i := 0; i < numOfEvents; i++ {
		insertedEvents[i] = db.GenerateRandomEvent()
		tc.ingestService.Queue <- insertedEvents[i]
	}

	Eventually(func() (int, error) {
		return tc.repo.GetEventsCount(ctx)
	}).WithContext(ctx).Should(Equal(numOfEvents), "Expected all events to be inserted")

	Eventually(func() ([]db.EventRecord, error) {
		return tc.repo.GetEvents(ctx)
	}).WithContext(ctx).Should(ConsistOf(insertedEvents), "Expected all events to match")
}

var _ = Describe("Event Record Insertion - 2 workers", func() {
	var tc *testContext

	BeforeEach(func(ctx SpecContext) {
		tc = setupTest(ctx, 2)
	})

	AfterEach(func() {
		tc.cleanup()
	})

	It("should successfully insert single event record", func(ctx SpecContext) {
		tc.testSingleEvent(ctx)
	})

	It("should successfully insert 10 event records", func(ctx SpecContext) {
		tc.testMultipleEvents(ctx)
	})
})

var _ = Describe("Event Record Insertion - 1 worker", func() {
	var tc *testContext

	BeforeEach(func(ctx SpecContext) {
		tc = setupTest(ctx, 1)
	})

	AfterEach(func() {
		tc.cleanup()
	})

	It("should successfully insert single event record", func(ctx SpecContext) {
		tc.testSingleEvent(ctx)
	})

	It("should successfully insert 10 event records", func(ctx SpecContext) {
		tc.testMultipleEvents(ctx)
	})
})
