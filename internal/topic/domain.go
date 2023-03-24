package topic

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"mengo/internal/constants"
	"mengo/internal/offset"
	"os"
)

type Topic struct {
	Name       string `json:"name"`
	Partitions uint8  `json:"partitions"`
}

func Create(database *mongo.Database, t *Topic) {
	collection := database.Collection(t.Name)
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	createIndexKeys := bson.D{
		{"partition", 1},
		{"offset", 1},
	}
	createIndexResult, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: createIndexKeys})
	if err != nil {
		fmt.Printf("Topic=%v creation failed with error=%v\n", t.Name, err)
		os.Exit(1)
	}
	fmt.Printf("Topic=%v creation successful with createIndexResult=%v\n", t.Name, createIndexResult)
	offset.CreateTopicOffsets(database, t.Name, t.Partitions)
}
