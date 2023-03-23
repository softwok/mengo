package boot

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"mengo/internal/constants"
	"mengo/internal/event"
)

func InitDatabase(database *mongo.Database) {
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	names, err := database.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		log.Fatalf("Collection names fetch failed with error: %v\n", err)
	}
	found := false
	for _, elem := range names {
		if elem == event.CollectionName {
			found = true
			break
		}
	}
	if !found {
		ctx, cancel = context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
		defer cancel()
		entityIdFieldName := "entityId"
		err = database.CreateCollection(ctx, event.CollectionName,
			&options.CreateCollectionOptions{TimeSeriesOptions: &options.TimeSeriesOptions{
				TimeField: "timestamp",
				MetaField: &entityIdFieldName,
			}})
		if err != nil {
			log.Fatalf("Events collection creation failed with error: %v\n", err)
		}
	}
}
