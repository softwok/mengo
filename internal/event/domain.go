package event

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"mengo/internal/constants"
	"time"
)

const CollectionName = "events"

type Event struct {
	Id        string    `bson:"_id" json:"id"`
	Event     string    `json:"event"`
	EntityId  string    `bson:"entityId" json:"entityId"`
	Timestamp time.Time `json:"timestamp"`
}

func (e *Event) Create(database *mongo.Database) {
	collection := database.Collection(CollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	result, err := collection.InsertOne(ctx, e)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Event inserted with id=%v\n", result.InsertedID)
}

func List(database *mongo.Database, timestamp *time.Time) []Event {
	collection := database.Collection(CollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	cursor, err := collection.Find(ctx, bson.D{{"timestamp", bson.D{{"$lt", timestamp}}}})
	if err != nil {
		panic(err)
	}
	var results []Event
	if err = cursor.All(context.TODO(), &results); err != nil {
		panic(err)
	}
	if results == nil {
		results = make([]Event, 0)
	}
	return results
}
