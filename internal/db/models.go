package db

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
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
