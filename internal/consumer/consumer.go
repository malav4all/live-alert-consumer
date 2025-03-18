package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// DeviceState tracks the latest state for each device
type DeviceState struct {
	LastDateTime time.Time
	LastMessage  []byte
}

// Consumer processes messages from Kafka
type Consumer struct {
	reader        *kafka.Reader
	writer        *kafka.Writer
	sourceTopic   string
	destTopic     string
	deviceStates  map[string]*DeviceState
	stateMutex    sync.RWMutex
	forwardBuffer map[string]bool
	bufferMutex   sync.Mutex
	forwardTicker *time.Ticker
}

// NewConsumer creates a new Consumer
func NewConsumer(brokers []string, sourceTopic, destTopic, groupID string) (*Consumer, error) {
	// Create reader with optimized settings
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       sourceTopic,
		GroupID:     groupID,
		MinBytes:    1e3,                    // 1KB minimum
		MaxBytes:    10e6,                   // 10MB maximum
		MaxWait:     100 * time.Millisecond, // Don't wait too long for a full batch
		StartOffset: kafka.LastOffset,       // Start from newest messages
	})

	// Create writer with optimized settings
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        destTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,      // For faster processing
		BatchSize:    100,                   // Batch multiple messages
		BatchTimeout: 10 * time.Millisecond, // Send quickly
	}

	consumer := &Consumer{
		reader:        reader,
		writer:        writer,
		sourceTopic:   sourceTopic,
		destTopic:     destTopic,
		deviceStates:  make(map[string]*DeviceState),
		forwardBuffer: make(map[string]bool),
		forwardTicker: time.NewTicker(200 * time.Millisecond), // Forward every 200ms
	}

	return consumer, nil
}

// Start starts the consumer
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("Started consuming from topic: %s", c.sourceTopic)

	// Start the forward goroutine
	go c.forwardLatestMessages(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping consumer for topic: %s", c.sourceTopic)
			c.forwardTicker.Stop()
			return nil
		default:
			// Read a message with a timeout
			readCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			msg, err := c.reader.FetchMessage(readCtx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded || err == context.Canceled {
					continue
				}
				log.Printf("Error reading from topic %s: %v", c.sourceTopic, err)
				time.Sleep(100 * time.Millisecond) // Short wait before retrying
				continue
			}

			// Process the message
			if err := c.processMessage(msg); err != nil {
				log.Printf("Error processing message from topic %s: %v", c.sourceTopic, err)
			}

			// Commit the message regardless of processing result
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Failed to commit message: %v", err)
			}
		}
	}
}

// processMessage processes a message and updates device state
func (c *Consumer) processMessage(msg kafka.Message) error {
	// Parse the message value
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Value, &data); err != nil {
		return fmt.Errorf("error unmarshaling message: %v", err)
	}

	// Extract IMEI
	imeiValue, ok := data["imei"]
	if !ok {
		return fmt.Errorf("IMEI not found in message data")
	}
	imei, ok := imeiValue.(string)
	if !ok {
		return fmt.Errorf("IMEI is not a string")
	}

	// Extract or generate dateTime in UTC ISO format
	var messageDateTime time.Time
	var dateTimeStr string

	// Try to get existing dateTime
	if dt, exists := data["dateTime"]; exists {
		dtStr, ok := dt.(string)
		if ok {
			parsed, err := time.Parse(time.RFC3339, dtStr)
			if err == nil {
				messageDateTime = parsed.UTC()
				dateTimeStr = messageDateTime.Format(time.RFC3339)
			} else {
				// Try other formats if RFC3339 fails
				formats := []string{
					"2006-01-02T15:04:05",
					"2006-01-02 15:04:05",
					"01/02/2006 15:04:05",
				}

				for _, format := range formats {
					parsed, err := time.Parse(format, dtStr)
					if err == nil {
						messageDateTime = parsed.UTC()
						dateTimeStr = messageDateTime.Format(time.RFC3339)
						break
					}
				}

				// Use current time if parsing fails
				if dateTimeStr == "" {
					messageDateTime = time.Now().UTC()
					dateTimeStr = messageDateTime.Format(time.RFC3339)
				}
			}
		} else {
			messageDateTime = time.Now().UTC()
			dateTimeStr = messageDateTime.Format(time.RFC3339)
		}
	} else if ts, exists := data["timestamp"]; exists {
		// Try timestamp as string
		tsStr, ok := ts.(string)
		if ok {
			parsed, err := time.Parse(time.RFC3339, tsStr)
			if err == nil {
				messageDateTime = parsed.UTC()
				dateTimeStr = messageDateTime.Format(time.RFC3339)
			} else {
				messageDateTime = time.Now().UTC()
				dateTimeStr = messageDateTime.Format(time.RFC3339)
			}
		} else if tsNum, ok := ts.(float64); ok {
			// Try timestamp as number
			messageDateTime = time.Unix(int64(tsNum), 0).UTC()
			dateTimeStr = messageDateTime.Format(time.RFC3339)
		} else {
			messageDateTime = time.Now().UTC()
			dateTimeStr = messageDateTime.Format(time.RFC3339)
		}
	} else {
		messageDateTime = time.Now().UTC()
		dateTimeStr = messageDateTime.Format(time.RFC3339)
	}

	// Always ensure dateTime field is in correct format
	data["dateTime"] = dateTimeStr

	// Add source topic and processed time
	data["source_topic"] = c.sourceTopic
	data["processed_time"] = time.Now().UTC().Format(time.RFC3339)

	// Re-encode the data
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshaling data: %v", err)
	}

	// Update device state with lock protection
	c.updateDeviceState(imei, messageDateTime, jsonData)

	return nil
}

// updateDeviceState updates the tracked state for a device
func (c *Consumer) updateDeviceState(imei string, messageTime time.Time, jsonData []byte) {
	c.stateMutex.Lock()
	defer c.stateMutex.Unlock()

	state, exists := c.deviceStates[imei]
	if !exists {
		// First message for this device
		c.deviceStates[imei] = &DeviceState{
			LastDateTime: messageTime,
			LastMessage:  jsonData,
		}

		// Mark for forwarding
		c.bufferMutex.Lock()
		c.forwardBuffer[imei] = true
		c.bufferMutex.Unlock()

		return
	}

	// Only update if this message is newer
	if messageTime.After(state.LastDateTime) {
		state.LastDateTime = messageTime
		state.LastMessage = jsonData

		// Mark for forwarding
		c.bufferMutex.Lock()
		c.forwardBuffer[imei] = true
		c.bufferMutex.Unlock()
	}
}

// forwardLatestMessages periodically forwards the latest message for each device
func (c *Consumer) forwardLatestMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.forwardTicker.C:
			c.forwardBufferedMessages(ctx)
		}
	}
}

// forwardBufferedMessages sends the latest message for each device in the buffer
func (c *Consumer) forwardBufferedMessages(ctx context.Context) {
	// Get devices to forward and clear buffer
	c.bufferMutex.Lock()
	devicesToForward := make([]string, 0, len(c.forwardBuffer))
	for imei := range c.forwardBuffer {
		devicesToForward = append(devicesToForward, imei)
	}
	c.forwardBuffer = make(map[string]bool)
	c.bufferMutex.Unlock()

	if len(devicesToForward) == 0 {
		return
	}

	// Prepare messages to send
	messages := make([]kafka.Message, 0, len(devicesToForward))

	c.stateMutex.RLock()
	for _, imei := range devicesToForward {
		if state, exists := c.deviceStates[imei]; exists {
			messages = append(messages, kafka.Message{
				Key:   []byte(imei),
				Value: state.LastMessage,
			})
		}
	}
	c.stateMutex.RUnlock()

	// Send messages in a batch if there are any
	if len(messages) > 0 {
		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		err := c.writer.WriteMessages(writeCtx, messages...)
		if err != nil {
			log.Printf("Error forwarding messages: %v", err)
		} else {
			log.Printf("Forwarded %d messages to topic %s", len(messages), c.destTopic)

			// Log individual messages at debug level
			for _, msg := range messages {
				var data map[string]interface{}
				if err := json.Unmarshal(msg.Value, &data); err != nil {
					continue
				}

				imei, _ := data["imei"].(string)
				dateTime, _ := data["dateTime"].(string)
				log.Printf("Forwarded message with IMEI %s and dateTime %s from topic %s to %s",
					imei, dateTime, c.sourceTopic, c.destTopic)
			}
		}
	}
}

// Close closes the consumer
func (c *Consumer) Close() error {
	c.forwardTicker.Stop()

	if err := c.reader.Close(); err != nil {
		return err
	}
	return c.writer.Close()
}
