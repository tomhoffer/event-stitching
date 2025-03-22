package main

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Main Suite")
}

var _ = Describe("Event Record Insertion", func() {

	var connPool *pgxpool.Pool

	BeforeEach(func(ctx SpecContext) {
		var err error
		connPool, err = pgxpool.New(ctx, "postgres://myuser:mypassword@localhost:5432/mydatabase")
		Expect(err).NotTo(HaveOccurred())

		// Clean up the database before each test
		_, err = connPool.Exec(ctx, "TRUNCATE TABLE events")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		connPool.Close()
	})

	It("should successfully insert single event record", func(ctx SpecContext) {
		generatedEvent := GenerateRandomEvent()
		eQueue := make(chan EventRecord, 1)
		eQueue <- generatedEvent
		close(eQueue)

		// Start the storeData goroutine
		go storeData(ctx, eQueue, connPool)

		// Wait for the record to be inserted
		Eventually(func() error {
			// Verify there is exactly one record inserted
			count, err := getEventsCount(ctx, connPool)
			if err != nil {
				return err
			}
			Expect(count).To(Equal(1))

			// Verify the record contents
			event, err := getEvents(ctx, connPool)
			Expect(err).NotTo(HaveOccurred())

			Expect(event[0].EventId).To(Equal(generatedEvent.EventId))
			Expect(event[0].Cookie).To(Equal(generatedEvent.Cookie))
			Expect(event[0].MessageId).To(Equal(generatedEvent.MessageId))
			Expect(event[0].Phone).To(Equal(generatedEvent.Phone))

			return nil
		}, "5s").Should(Succeed())
	})

	It("should successfully insert 10 event records", func(ctx SpecContext) {
		numOfEvents := 10
		eQueue := make(chan EventRecord, numOfEvents)
		insertedEvents := [10]EventRecord{}

		for i := 0; i < numOfEvents; i++ {
			insertedEvents[i] = GenerateRandomEvent()
			eQueue <- insertedEvents[i]
		}
		close(eQueue)

		// Start the storeData goroutine
		go storeData(ctx, eQueue, connPool)

		Eventually(func() (int, error) {
			return getEventsCount(ctx, connPool)
		}).WithContext(ctx).Should(Equal(numOfEvents))

		Eventually(func() ([]EventRecord, error) {
			return getEvents(ctx, connPool)
		}, "5s").WithContext(ctx).Should(HaveExactElements(insertedEvents))
	})
})
