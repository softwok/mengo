package offset

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"mengo/internal/constants"
	"sync"
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
	UncommittedOffset uint32 `bson:"uncommittedOffset"`
}

type TopicPartition struct {
	Topic     string
	Partition uint8
}

type TopicOffset struct {
	Id     TopicPartition `bson:"_id"`
	Offset uint32
}

type CommitRequest struct {
	Topic         string `json:"topic"`
	ConsumerGroup string `json:"consumerGroup"`
	Partition     uint8  `json:"partition"`
}

type TopicOffsetCache struct {
	Offset uint32
	Mu     sync.Mutex
}

func GetConsumerGroupOffset(database *mongo.Database, consumerGroup string, topic string, partition uint8) ConsumerGroupOffset {
	collection := database.Collection(ConsumerGroupOffsetCollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	consumerGroupTopicPartition := ConsumerGroupTopicPartition{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		Partition:     partition,
	}
	filter := bson.D{{"_id", consumerGroupTopicPartition}}
	result := collection.FindOne(ctx, filter)
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			return ConsumerGroupOffset{
				Id:                consumerGroupTopicPartition,
				Offset:            0,
				UncommittedOffset: 0,
			}
		}
		panic(result.Err())
	}
	var consumerGroupOffset ConsumerGroupOffset
	err := result.Decode(&consumerGroupOffset)
	if err != nil {
		panic(err)
	}
	return consumerGroupOffset
}

func SetUncommittedConsumerGroupOffset(database *mongo.Database, consumerGroup string, topic string, partition uint8, uncommittedOffset uint32) {
	collection := database.Collection(ConsumerGroupOffsetCollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	consumerGroupTopicPartition := ConsumerGroupTopicPartition{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		Partition:     partition,
	}
	filter := bson.D{{"_id", consumerGroupTopicPartition}}
	update := bson.D{{"$set", bson.D{{"uncommittedOffset", uncommittedOffset}}}}
	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		panic(err)
	}
	if result.ModifiedCount == 0 && result.MatchedCount == 0 {
		consumerGroupOffset := ConsumerGroupOffset{
			Id:                consumerGroupTopicPartition,
			Offset:            0,
			UncommittedOffset: uncommittedOffset,
		}
		result, err := collection.InsertOne(ctx, consumerGroupOffset)
		if err != nil {
			panic(err)
		}
		fmt.Printf("ConsumerGroupOffset set with result=%v\n", result)
	} else {
		fmt.Printf("ConsumerGroupOffset set with result=%v\n", result)
	}
}

func Commit(database *mongo.Database, request *CommitRequest) {
	consumerGroupOffset := GetConsumerGroupOffset(database, request.ConsumerGroup, request.Topic, request.Partition)
	collection := database.Collection(ConsumerGroupOffsetCollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	consumerGroupTopicPartition := ConsumerGroupTopicPartition{
		ConsumerGroup: request.ConsumerGroup,
		Topic:         request.Topic,
		Partition:     request.Partition,
	}
	filter := bson.D{{"_id", consumerGroupTopicPartition}}
	update := bson.D{{"$set", bson.D{
		{"offset", consumerGroupOffset.UncommittedOffset},
		{"uncommittedOffset", nil},
	}}}
	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		panic(err)
	}
	fmt.Printf("ConsumerGroupOffset set with result=%v\n", result)
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

func UpdateTopicOffset(database *mongo.Database, topic string, partition uint8, offset uint32) {
	collection := database.Collection(TopicOffsetCollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	filter := bson.D{{"_id", TopicPartition{Topic: topic, Partition: partition}}}
	update := bson.D{{"$set", bson.D{
		{"offset", offset},
	}}}
	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		panic(err)
	}
	fmt.Printf("TopicOffset set with result=%v\n", result)
}

func GetTopicOffsetMap(database *mongo.Database) *map[TopicPartition]*TopicOffsetCache {
	collection := database.Collection(TopicOffsetCollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	filter := bson.D{{}}
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		panic(err)
	}
	var topicOffsets []TopicOffset
	if err = cursor.All(ctx, &topicOffsets); err != nil {
		panic(err)
	}
	if topicOffsets == nil {
		topicOffsets = make([]TopicOffset, 0)
	}
	topicPartitionMap := make(map[TopicPartition]*TopicOffsetCache)
	for _, topicOffset := range topicOffsets {
		topicPartitionMap[topicOffset.Id] = &TopicOffsetCache{Offset: topicOffset.Offset}
	}
	return &topicPartitionMap
}
