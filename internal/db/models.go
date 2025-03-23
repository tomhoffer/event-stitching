package db

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/google/uuid"
)

type EventIdentifier struct {
	Cookie    string `db:"cookie"`
	MessageId string `db:"message_id"`
	Phone     string `db:"phone"`
}

func (e EventIdentifier) GetIdentifierNames() []string {
	identifierNames := []string{}
	t := reflect.TypeOf(e)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		dbTag := field.Tag.Get("db")
		if dbTag == "" {
			continue
		}
		identifierNames = append(identifierNames, dbTag)
	}
	return identifierNames
}

func (e EventIdentifier) GetIdentifierValueByName(name string) (value string, found bool) {
	t := reflect.TypeOf(e)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		dbTag := field.Tag.Get("db")
		if dbTag == "" {
			continue
		}
		if dbTag == name {
			return reflect.ValueOf(e).Field(i).String(), true
		}
	}
	return "", false
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
		EventId:        rand.Intn(100),
		EventTimestamp: time.Now().UTC().Add(time.Duration(rand.Intn(1000)) * time.Millisecond),
	}
}

type Profile struct {
	Cookie    string `db:"cookie"`
	MessageId string `db:"message_id"`
	Phone     string `db:"phone"`
}
