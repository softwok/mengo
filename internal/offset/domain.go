package offset

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"mengo/internal/constants"
)

const ConsumerGroupOffsetCollectionName = "consumerGroupOffsets"
const TopicOffsetCollectionName = "topicOffsets"

type ConsumerGroupTopicPartition struct {
	ConsumerGroup string `bson:"consumerGroup"`
	Topic         string
	Partition     uint8
}

type ConsumerGroupOffset struct {
	Id                ConsumerGroupTopicPartition `bson:"_id"`
	Offset            uint32
	UncommittedOffset uint32
}

type TopicPartition struct {
	Topic     string
	Partition uint8
}

type TopicOffset struct {
	Id     TopicPartition `bson:"_id"`
	Offset uint32
}

func GetConsumerGroupOffset(database *mongo.Database, consumerGroup string, topic string, partition uint8) uint32 {
	collection := database.Collection(ConsumerGroupOffsetCollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	consumerGroupTopic := ConsumerGroupTopicPartition{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		Partition:     partition,
	}
	result := collection.FindOne(ctx, consumerGroupTopic)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			return 0
		}
		panic(result.Err())
	}
	var consumerGroupOffset ConsumerGroupOffset
	err := result.Decode(&consumerGroupOffset)
	if err != nil {
		panic(err)
	}
	return consumerGroupOffset.Offset
}

func CreateTopicOffsets(database *mongo.Database, topic string, partitions uint8) {
	collection := database.Collection(TopicOffsetCollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	topicOffsets := make([]TopicOffset, partitions)
	for i := uint8(0); i < partitions; i++ {
		topicOffsets[i] = TopicOffset{
			Id: TopicPartition{
				Topic:     topic,
				Partition: i,
			},
			Offset: 0,
		}
	}
	var topicOffsetInterfaces []interface{}
	for _, topicOffset := range topicOffsets {
		topicOffsetInterfaces = append(topicOffsetInterfaces, topicOffset)
	}
	result, err := collection.InsertMany(ctx, topicOffsetInterfaces)
	if err != nil {
		panic(err)
	}
	fmt.Printf("TopicOffsets inserted with ids=%v\n", result.InsertedIDs)
}

func GetTopicOffset(database *mongo.Database, topic string, partition uint8) uint32 {
	collection := database.Collection(TopicOffsetCollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	topicPartition := TopicPartition{
		Topic:     topic,
		Partition: partition,
	}
	filter := bson.D{{"_id", topicPartition}}
	result := collection.FindOne(ctx, filter)
	if result.Err() != nil {
		panic(result.Err())
	}
	var topicOffset TopicOffset
	err := result.Decode(&topicOffset)
	if err != nil {
		panic(err)
	}
	return topicOffset.Offset
}

func IncrementTopicOffset(database *mongo.Database, topic string, partition uint8) {
	collection := database.Collection(TopicOffsetCollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	topicPartition := TopicPartition{
		Topic:     topic,
		Partition: partition,
	}
	filter := bson.D{{"_id", topicPartition}}
	update := bson.D{{"$inc", bson.D{{"offset", 1}}}}
	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		panic(err)
	}
	fmt.Printf("TopicOffsets incremented with result=%v\n", result)
}
