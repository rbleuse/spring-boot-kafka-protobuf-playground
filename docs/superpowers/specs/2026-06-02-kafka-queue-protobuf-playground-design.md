# Kafka Queue Protobuf Playground Design

## Goal

Build a small Spring Boot and Kotlin playground for Apache Kafka 4.2 queue
features. The application exposes one HTTP endpoint that publishes protobuf
messages to a Kafka topic and processes them with a Spring Kafka share-group
listener. The sample favors clarity over extensibility.

## API

Expose `POST /messages`.

The request body is JSON with a `type` discriminator. Jackson maps it to a
sealed Kotlin request hierarchy with four variants:

- `email`: recipient and subject
- `sms`: phone number and text
- `push`: device token and title
- `audit`: actor and action

The controller converts the request to a generated protobuf message, publishes
it to Kafka, and returns `202 Accepted`.

## Protobuf Model

Define a `QueueMessage` protobuf envelope with an `id` and a `oneof payload`.
The payload variants are `Email`, `Sms`, `Push`, and `Audit`.

Use generated protobuf Java classes from Kotlin. A small mapper owns the
conversion between HTTP request types and the generated envelope.

## Kafka Queue Processing

Publish serialized `QueueMessage` bytes to `playground.queue`.

Configure Spring Kafka's `ShareKafkaListenerContainerFactory` and consume the
topic with one share-group listener. This exercises Kafka queue semantics while
keeping the demo intentionally small. The listener logs processed messages and
stores them in a simple in-memory observer so integration tests can await
processing without testing log output.

## Deserialization Failure Handling

Use a custom protobuf deserializer that calls generated
`QueueMessage.parseFrom(bytes)`.

Malformed protobuf bytes must not reach the application listener. Route them
immediately, without retries, to `playground.queue.DLT`. Processing must
continue so a later valid record is still handled.

The DLT contains the original record bytes and Kafka headers describing the
failure. The playground does not add a DLT consumer.

## Local Development

Add Spring Boot Docker Compose support as a development-only dependency.
Provide `compose.yaml` with a single Apache Kafka broker using the requested
`apache/kafka:4.2.1` image if that image is available. Spring Boot starts the
Compose service for local application runs and discovers its mapped port.

If the requested image tag is unavailable, use the newest available Apache
Kafka 4.2.x image and document the concrete tag.

## Testing

Use Testcontainers with its Apache Kafka container and Kotest assertions.
Integration tests start a Kafka broker using the same Apache Kafka 4.2.x image
as local development and prove:

1. Each JSON request variant is accepted, published, and processed.
2. Malformed protobuf bytes are published to `playground.queue.DLT`.
3. A malformed record does not prevent a later valid record from being
   processed.

Keep unit tests narrow: test the HTTP-to-protobuf mapper only if integration
tests do not cover its branches clearly.

## Out Of Scope

- Multiple application instances or competing listener instances
- Schema Registry
- Authentication or authorization
- Retry policies beyond immediate DLT routing
- Persisting processed messages
- DLT replay tooling
