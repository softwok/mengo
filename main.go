package main

import (
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"mengo/internal/constants"
	"mengo/internal/event"
	"mengo/internal/offset"
	"mengo/internal/poll"
	"mengo/internal/topic"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	databaseName = "mengo"
)

var (
	mongoDBUri = os.Getenv("MONGO_DB_URI")
)

func main() {
	// MongoDB
	opts := options.Client().ApplyURI(mongoDBUri).SetConnectTimeout(constants.DatabaseTimeOut)
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		log.Fatalf("MongoDB connection failed with error=%v\n", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
		defer cancel()
		if err = client.Disconnect(ctx); err != nil {
			log.Fatalf("MongoDB disconnection failed with error=%v\n", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), constants.DatabaseTimeOut)
	defer cancel()
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatalf("MongoDB ping failed with error=%v\n", err)
	}
	database := client.Database(databaseName)
	topicOffsetMap := offset.GetTopicOffsetMap(database)
	event.RebuildLatestOffsets(database, topicOffsetMap)
	// Create a new HTTP server
	srv := &http.Server{
		Addr: ":8080",
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			handleRequest(writer, request, database, topicOffsetMap)
		}),
	}

	// Start the server in a separate goroutine
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not listen on %s=%v\n", srv.Addr, err)
		}
	}()

	// Create a signal channel to receive termination signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Wait for a signal to shut down the server
	<-signalChan

	// Create a context with a timeout of 5 seconds
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Shut down the server gracefully
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Could not gracefully shut down the server=%v\n", err)
	}

	// Log a message indicating that the server has shut down
	log.Println("Server gracefully shut down")
}

func handleRequest(w http.ResponseWriter, r *http.Request, database *mongo.Database,
	topicOffsetMap *map[offset.TopicPartition]*offset.TopicOffsetCache) {
	if r.URL.Path == "/create-topic" && r.Method == http.MethodPost {
		var model topic.Topic
		if err := json.NewDecoder(r.Body).Decode(&model); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
		log.Printf("Request received=%v\n", model)
		topic.Create(database, &model)
	} else if r.URL.Path == "/poll" && r.Method == http.MethodPost {
		var model poll.Request
		if err := json.NewDecoder(r.Body).Decode(&model); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
		log.Printf("Request received=%v\n", model)
		events := event.List(database, model.ConsumerGroup, model.Topic, model.Partition)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(events); err != nil {
			http.Error(w, "Serialization error", http.StatusInternalServerError)
			return
		}
	} else if r.URL.Path == "/notify" && r.Method == http.MethodPost {
		var model event.Payload
		if err := json.NewDecoder(r.Body).Decode(&model); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
		log.Printf("Request received=%v\n", model)
		offsetCache := (*topicOffsetMap)[offset.TopicPartition{
			Topic:     model.Topic,
			Partition: model.Partition,
		}]
		event.Persist(database, &model, offsetCache)
	} else if r.URL.Path == "/commit" && r.Method == http.MethodPost {
		var model offset.CommitRequest
		if err := json.NewDecoder(r.Body).Decode(&model); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
		log.Printf("Request received=%v\n", model)
		offset.Commit(database, &model)
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
