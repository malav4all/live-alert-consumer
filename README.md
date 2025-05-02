# Live Alert Consumer

A Kafka consumer application that processes alert messages from multiple source topics and forwards them to a destination topic.

## Overview

This application consumes alert messages from configurable Kafka source topics, processes them, and produces the processed alert messages to a destination topic. It's designed to handle multiple source topics concurrently and ensure graceful shutdown when terminated.

## Features

- Consumption of alert messages from multiple source topics
- Concurrent processing using goroutines
- Graceful shutdown on termination signals
- Configurable via environment variables or .env file
- Automatic topic creation if not exists

## Prerequisites

- Go 1.16+
- Access to a Kafka cluster
- Git (for cloning the repository)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/malav4all/live-alert-consumer.git
   cd live-alert-consumer
   ```

2. Install dependencies:
   ```bash
   go mod download
   ```

## Configuration

The application can be configured using environment variables or a `.env` file. Create a `.env` file in the root directory with the following variables:

```
# Kafka configuration
KAFKA_BROKERS=103.20.212.44:9092
SOURCE_TOPICS=alert_508_jsonData,alert700json
DESTINATION_TOPIC=live_alerts
CONSUMER_GROUP_ID=live-alert-consumer
```

### Configuration Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BROKERS` | Comma-separated list of Kafka broker addresses | `103.20.212.44:9092` |
| `SOURCE_TOPICS` | Comma-separated list of source topic names | `alert_508_jsonData,alert700json` |
| `DESTINATION_TOPIC` | Name of the destination topic | `live_alerts` |
| `CONSUMER_GROUP_ID` | Kafka consumer group ID | `live-alert-consumer` |

## Running the Application

1. Build the application:
   ```bash
   go build -o live-alert-consumer
   ```

2. Run the application:
   ```bash
   ./live-alert-consumer
   ```

Alternatively, you can run it directly without building:
```bash
go run main.go
```

## Docker Support

### Building the Docker Image

```bash
docker build -t live-alert-consumer .
```

### Running the Docker Container

```bash
docker run --env-file .env live-alert-consumer
```

## Development

### Project Structure

```
├── config/
│   └── config.go         # Configuration handling
├── internal/
│   ├── consumer/         # Kafka consumer implementation
│   └── kafka/            # Kafka utilities
├── .env                  # Environment configuration
├── main.go               # Application entry point
└── README.md             # This file
```

### Adding a New Source Topic

To add a new source topic, simply add it to the `SOURCE_TOPICS` environment variable or update the default in `config.go`.

## Troubleshooting

### Common Issues

- **Connection Refused**: Ensure that the Kafka broker is running and accessible from your network.
- **Topic Not Found**: The application will attempt to create the destination topic if it doesn't exist, but ensure you have the necessary permissions.
- **Permission Denied**: Ensure you have the necessary permissions to create and access topics in your Kafka cluster.

## License

[MIT License](LICENSE)

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request