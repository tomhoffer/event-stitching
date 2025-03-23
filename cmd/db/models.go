package db

import "time"

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
