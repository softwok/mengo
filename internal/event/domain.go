package event

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"mengo/internal/constants"
	"mengo/internal/offset"
	"time"
)

type Event struct {
	Id        string    `bson:"_id" json:"id"`
	Partition uint8     `json:"partition"`
	Offset    uint32    `json:"offset"`
	Event     string    `json:"event"`
	EntityId  string    `bson:"entityId" json:"entityId"`
	Timestamp time.Time `json:"timestamp"`
}

type Payload struct {
	Id        string    `json:"id"`
	Topic     string    `json:"topic"`
	Partition uint8     `json:"partition"`
	Event     string    `json:"event"`
	EntityId  string    `json:"entityId"`
	Timestamp time.Time `json:"timestamp"`
}

func Persist(database *mongo.Database, payload *Payload) {
	// The whole method should be transactional
	collection := database.Collection(payload.Topic)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	o := offset.GetTopicOffset(database, payload.Topic, payload.Partition)
	e := Event{
		Id:        payload.Id,
		Partition: payload.Partition,
		Offset:    o,
		Event:     payload.Event,
		EntityId:  payload.EntityId,
		Timestamp: payload.Timestamp,
	}
	result, err := collection.InsertOne(ctx, e)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Event inserted with id=%v\n", result.InsertedID)
	offset.IncrementTopicOffset(database, payload.Topic, payload.Partition)
}

func List(database *mongo.Database, consumerGroup string, topic string, partition uint8) []Event {
	collection := database.Collection(topic)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	consumerGroupOffset := offset.GetConsumerGroupOffset(database, consumerGroup, topic, partition)
	cursor, err := collection.Find(ctx, bson.D{
		{"partition", partition},
		{"offset", bson.D{{"$gte", consumerGroupOffset.Offset}}},
	})
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
	if len(results) > 0 {
		last := results[len(results)-1]
		offset.SetUncommittedConsumerGroupOffset(database, consumerGroup, topic, partition, last.Offset+1)
	}
	return results
}
