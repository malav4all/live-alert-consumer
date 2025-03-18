package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// Writer is a wrapper around kafka.Writer with additional functionality
type Writer struct {
	writer *kafka.Writer
	topic  string
}

// NewWriter creates a new Writer for a specific topic
func NewWriter(brokers []string, topic string) *Writer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Use hash balancer for consistent partitioning
		RequiredAcks: kafka.RequireAll,
	}

	return &Writer{
		writer: writer,
		topic:  topic,
	}
}

// WriteMessage writes a message to Kafka
func (w *Writer) WriteMessage(ctx context.Context, key, value []byte) error {
	err := w.writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("error writing message to destination: %v", err)
	}
	return nil
}

// Close closes the writer
func (w *Writer) Close() error {
	return w.writer.Close()
}

// Topic returns the topic this writer is publishing to
func (w *Writer) Topic() string {
	return w.topic
}

// EnsureTopic checks if a Kafka topic exists, and creates it if it does not
func EnsureTopic(brokers []string, topic string) error {
	// Connect to the Kafka broker
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka broker: %v", err)
	}
	defer conn.Close()

	// Get list of existing topics
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to get Kafka topics: %v", err)
	}

	// Check if the topic already exists
	for _, p := range partitions {
		if p.Topic == topic {
			log.Printf("Kafka topic %s already exists", topic)
			return nil
		}
	}

	// Define topic creation request
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		},
	}

	// Create the topic
	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("failed to create Kafka topic %s: %v", topic, err)
	}

	log.Printf("Created Kafka topic: %s", topic)
	return nil
}
