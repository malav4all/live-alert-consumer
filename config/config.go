package config

import (
	"os"
	"strings"
)

// Config holds the application configuration
type Config struct {
	KafkaBrokers     []string
	SourceTopics     []string
	DestinationTopic string
	ConsumerGroupID  string
}

// LoadConfig loads the configuration from environment variables with defaults
func LoadConfig() *Config {
	// Default configuration
	config := &Config{
		KafkaBrokers:     []string{"103.20.212.44:9092"},
		SourceTopics:     []string{"socket_508_jsonData"},
		DestinationTopic: "live_tracking",
		ConsumerGroupID:  "live-tracking-consumer",
	}

	// Override with environment variables if present
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		config.KafkaBrokers = strings.Split(brokers, ",")
	}
	if topics := os.Getenv("SOURCE_TOPICS"); topics != "" {
		config.SourceTopics = strings.Split(topics, ",")
	}
	if topic := os.Getenv("DESTINATION_TOPIC"); topic != "" {
		config.DestinationTopic = topic
	}
	if groupID := os.Getenv("CONSUMER_GROUP_ID"); groupID != "" {
		config.ConsumerGroupID = groupID
	}

	return config
}
