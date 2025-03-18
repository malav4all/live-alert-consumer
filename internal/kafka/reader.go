package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Reader is a wrapper around kafka.Reader with additional functionality
type Reader struct {
	reader *kafka.Reader
	topic  string
}

// NewReader creates a new Reader for a specific topic
func NewReader(brokers []string, topic, groupID string) *Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MaxBytes: 10e6,
	})

	return &Reader{
		reader: reader,
		topic:  topic,
	}
}

// ReadMessage reads a message from Kafka with a timeout
func (r *Reader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	msg, err := r.reader.FetchMessage(readCtx)
	if err != nil {
		return kafka.Message{}, err
	}

	return msg, nil
}

// CommitMessage commits a message as processed
func (r *Reader) CommitMessage(ctx context.Context, msg kafka.Message) error {
	if err := r.reader.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to commit message: %v", err)
	}
	return nil
}

// Close closes the reader
func (r *Reader) Close() error {
	return r.reader.Close()
}

// Topic returns the topic this reader is consuming from
func (r *Reader) Topic() string {
	return r.topic
}
