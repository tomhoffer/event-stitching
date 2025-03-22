package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventIdentifier struct {
	Cookie    string `db:"cookie"`
	MessageId string `db:"message_id"`
	Phone     string `db:"phone"`
}

type EventRecord struct {
	EventIdentifier
	EventId        int       `db:"event_id"`
	EventTimestamp time.Time `db:"event_timestamp"`
}

func GenerateRandomEvent() EventRecord {
	return EventRecord{
		EventIdentifier: EventIdentifier{
			Cookie:    uuid.New().String(),
			MessageId: uuid.New().String(),
			Phone:     fmt.Sprintf("+1%09d", rand.Intn(1e9)),
		},
		EventId:        rand.Intn(1000),
		EventTimestamp: time.Now().UTC().Add(time.Duration(rand.Intn(1000)) * time.Millisecond),
	}
}

func storeData(ctx context.Context, queue <-chan EventRecord, conn *pgxpool.Pool) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Task cancelled")
			return
		case c, ok := <-queue:
			if !ok {
				fmt.Println("Queue closed, exiting...")
				return
			}

			// TODO SET lowest idenifier as profile_id, create medzitable
			// TODO Identity resolution to zoraruje podla event_timestamp a vzdy to spracuje podla tomu poradia

			t, err := conn.Exec(ctx,
				"INSERT INTO events (event_id, event_timestamp, identifiers) VALUES ($1, $2, $3)",
				c.EventId,
				c.EventTimestamp,
				map[string]interface{}{
					"cookie":     c.Cookie,
					"message_id": c.MessageId,
					"phone":      c.Phone,
				})
			if err != nil {
				fmt.Printf("Failed to insert data: %v\n", err)
				continue
			}
			fmt.Println("Insert successful:", t)
		}
	}
}

func insertRecords(ctx context.Context, connPool *pgxpool.Pool) {
	cQueue := make(chan EventRecord, 100)

	var wg sync.WaitGroup

	numInsertWorkers := 2
	wg.Add(numInsertWorkers) // Wait for all workers

	for i := 0; i < numInsertWorkers; i++ {
		go func() {
			storeData(ctx, cQueue, connPool)
			wg.Done() // Mark worker as done
		}()
	}
	for i := 0; i < 100; i++ {
		cQueue <- GenerateRandomEvent()
	}

	close(cQueue) // Close queue so workers stop
	wg.Wait()     // Wait for all workers to finish
}

func runStitching(ctx context.Context, connPool *pgxpool.Pool) {
	const stitchingInterval = 5 * time.Second
	const stitchingWorkers = 2
	const stitchingBatchSize = 10

	for i := 0; i < stitchingWorkers; i++ {
		go stitchWorker(ctx, connPool, stitchingBatchSize, stitchingInterval)
	}
}

func stitchWorker(ctx context.Context, connPool *pgxpool.Pool, batchSize int, stitchingInterval time.Duration) {
	for {
		stitch(ctx, connPool, batchSize)
		time.Sleep(stitchingInterval)
	}
}

func stitch(ctx context.Context, connPool *pgxpool.Pool, batchSize int) {
	// First query: Select and lock rows for processing
	rows, err := connPool.Query(ctx,
		"SELECT cookie, message_id, phone FROM profiles WHERE stitched = false LIMIT $1 FOR UPDATE",
		batchSize)
	if err != nil {
		fmt.Printf("Failed to query unstitched rows: %v\n", err)
		return
	}
	defer rows.Close()

	// Process the rows
	for rows.Next() {
		var cookie, messageId, phone string
		if err := rows.Scan(&cookie, &messageId, &phone); err != nil {
			fmt.Printf("Failed to scan row: %v\n", err)
			continue
		}

		// Second query: Mark the row as stitched
		_, err = connPool.Exec(ctx,
			"UPDATE profiles SET stitched = true WHERE cookie = $1 AND message_id = $2 AND phone = $3",
			cookie, messageId, phone)
		if err != nil {
			fmt.Printf("Failed to mark row as stitched: %v\n", err)
			continue
		}
	}
}

func getEvents(ctx context.Context, connPool *pgxpool.Pool) ([]EventRecord, error) {
	rows, err := connPool.Query(ctx, `
		SELECT 
			event_id,
			event_timestamp,
			identifiers->>'cookie' as cookie,
			identifiers->>'message_id' as message_id,
			identifiers->>'phone' as phone
		FROM events`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[EventRecord])
}

func getEventsCount(ctx context.Context, connPool *pgxpool.Pool) (int, error) {
	var count int
	err := connPool.QueryRow(ctx, "SELECT COUNT(*) FROM events").Scan(&count)
	return count, err
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	//conn, err := pgx.Connect(ctx, "postgres://myuser:mypassword@localhost:5432/mydatabase")
	connPool, err := pgxpool.New(ctx, "postgres://myuser:mypassword@localhost:5432/mydatabase")
	if err != nil {
		fmt.Printf("Unable to connect: %v\n", err)
		os.Exit(1)
	}
	defer connPool.Close()

	insertRecords(ctx, connPool)

}
