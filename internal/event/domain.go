package event

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

type PartitionMaxOffset struct {
	Partition uint8  `bson:"_id"`
	MaxOffset uint32 `bson:"maxOffset"`
}

func getTopics(topicOffsetMap *map[offset.TopicPartition]*offset.TopicOffsetCache) []string {
	topicSet := make(map[string]bool)
	for topicPartition := range *topicOffsetMap {
		topicSet[topicPartition.Topic] = true
	}
	topics := make([]string, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}
	return topics
}

func RebuildLatestOffsets(database *mongo.Database, topicOffsetMap *map[offset.TopicPartition]*offset.TopicOffsetCache) {
	topics := getTopics(topicOffsetMap)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	for _, topic := range topics {
		collection := database.Collection(topic)
		pipe := []bson.M{{"$group": bson.M{"_id": "$partition", "maxOffset": bson.M{"$max": "$offset"}}}}
		cursor, err := collection.Aggregate(ctx, pipe)
		if err != nil {
			panic(err)
		}
		var results []PartitionMaxOffset
		if err = cursor.All(ctx, &results); err != nil {
			panic(err)
		}
		if results == nil {
			results = make([]PartitionMaxOffset, 0)
		}
		for _, result := range results {
			topicOffsetCache := (*topicOffsetMap)[offset.TopicPartition{Topic: topic, Partition: result.Partition}]
			fmt.Printf("Topic=%v has offset in events=%v and in cache=%v\n", topic, result.MaxOffset,
				topicOffsetCache.Offset)
			if topicOffsetCache.Offset < result.MaxOffset {
				topicOffsetCache.Offset = result.MaxOffset + 1
				offset.UpdateTopicOffset(database, topic, result.Partition, topicOffsetCache.Offset)
			}
		}
	}
}

func Persist(database *mongo.Database, payload *Payload, topicOffsetCache *offset.TopicOffsetCache) {
	topicOffsetCache.Mu.Lock()
	defer topicOffsetCache.Mu.Unlock()
	collection := database.Collection(payload.Topic)
	e := Event{
		Id:        payload.Id,
		Partition: payload.Partition,
		Offset:    topicOffsetCache.Offset,
		Event:     payload.Event,
		EntityId:  payload.EntityId,
		Timestamp: payload.Timestamp,
	}
	if e.Id == "" {
		e.Id = primitive.NewObjectID().Hex()
	}
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	result, err := collection.InsertOne(ctx, e)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Event inserted with id=%v\n", result)
	topicOffsetCache.Offset++
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
	if err = cursor.All(ctx, &results); err != nil {
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
