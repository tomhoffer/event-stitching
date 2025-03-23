package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tomashoffer/event-stitching/cmd/db"
)

func storeData(ctx context.Context, queue <-chan db.EventRecord, repo *db.Repository) {
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
			if err := repo.InsertEvent(ctx, c); err != nil {
				fmt.Printf("Failed to insert data: %v\n", err)
				continue
			}
			fmt.Println("Insert successful")
		}
	}
}

func insertRecords(ctx context.Context, repo *db.Repository) {
	cQueue := make(chan db.EventRecord, 100)

	var wg sync.WaitGroup

	numInsertWorkers := 2
	wg.Add(numInsertWorkers)

	for i := 0; i < numInsertWorkers; i++ {
		go func() {
			storeData(ctx, cQueue, repo)
			wg.Done()
		}()
	}
	for i := 0; i < 100; i++ {
		cQueue <- db.GenerateRandomEvent()
	}

	close(cQueue)
	wg.Wait()
}

func runStitching(ctx context.Context, repo *db.Repository) {
	const stitchingInterval = 5 * time.Second
	const stitchingWorkers = 2
	const stitchingBatchSize = 10

	for i := 0; i < stitchingWorkers; i++ {
		go stitchWorker(ctx, repo, stitchingBatchSize, stitchingInterval)
	}
}

func stitchWorker(ctx context.Context, repo *db.Repository, batchSize int, stitchingInterval time.Duration) {
	for {
		stitch(ctx, repo, batchSize)
		time.Sleep(stitchingInterval)
	}
}

func stitch(ctx context.Context, repo *db.Repository, batchSize int) {
	events, err := repo.GetUnstitchedEvents(ctx, batchSize)
	if err != nil {
		fmt.Printf("Failed to query unstitched events: %v\n", err)
		return
	}

	for _, event := range events {
		if err := repo.MarkEventAsStitched(ctx, event); err != nil {
			fmt.Printf("Failed to mark event as stitched: %v\n", err)
			continue
		}
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connPool, err := pgxpool.New(ctx, "postgres://myuser:mypassword@localhost:5432/mydatabase")
	if err != nil {
		fmt.Printf("Unable to connect: %v\n", err)
		os.Exit(1)
	}
	defer connPool.Close()

	repo := db.NewRepository(connPool)
	insertRecords(ctx, repo)
}
