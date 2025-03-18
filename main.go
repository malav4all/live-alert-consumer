package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/malav4all/live-tracking-consumer/config"
	"github.com/malav4all/live-tracking-consumer/internal/consumer"
	"github.com/malav4all/live-tracking-consumer/internal/kafka"
)

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Log configuration
	log.Printf("Kafka Brokers: %v", cfg.KafkaBrokers)
	log.Printf("Source Topics: %v", cfg.SourceTopics)
	log.Printf("Destination Topic: %s", cfg.DestinationTopic)
	log.Printf("Consumer Group ID: %s", cfg.ConsumerGroupID)

	// Setup signal handling for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Ensure the destination topic exists
	if err := kafka.EnsureTopic(cfg.KafkaBrokers, cfg.DestinationTopic); err != nil {
		log.Fatalf("Failed to ensure destination topic exists: %v", err)
	}

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Create a consumer for each source topic
	for _, topic := range cfg.SourceTopics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()

			// Create a consumer
			consumer, err := consumer.NewConsumer(
				cfg.KafkaBrokers,
				topic,
				cfg.DestinationTopic,
				cfg.ConsumerGroupID,
			)
			if err != nil {
				log.Printf("Failed to create consumer for topic %s: %v", topic, err)
				return
			}
			defer consumer.Close()

			// Start the consumer
			if err := consumer.Start(ctx); err != nil {
				log.Printf("Consumer for topic %s exited with error: %v", topic, err)
			}
		}(topic)
	}

	log.Println("Consumer started. Press Ctrl+C to stop.")

	// Wait for termination signal
	<-ctx.Done()
	log.Println("Received termination signal. Shutting down...")

	// Wait for all consumers to finish
	wg.Wait()
	log.Println("All consumers shut down. Exiting.")
}
