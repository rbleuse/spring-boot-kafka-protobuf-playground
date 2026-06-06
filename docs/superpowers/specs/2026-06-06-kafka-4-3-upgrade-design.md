# Kafka 4.3 Upgrade Design

## Goal

Upgrade every local Kafka runtime from `apache/kafka:4.2.1` to
`apache/kafka:4.3.0` and simplify share-group provisioning by relying on the
new image defaults where they satisfy this single-node playground.

## Scope

The upgrade covers both supported runtime paths:

- Spring Boot Docker Compose support through `compose.yaml`.
- The Kafka Testcontainers fixture in `KafkaQueueIntegrationTests`.

Application queue behavior remains unchanged: the share group
`playground.queue.workers` consumes `playground.queue`, and malformed protobuf
records are sent to `playground.queue.DLT`.

## Kafka 4.3 Defaults

The `apache/kafka:4.3.0` image reports `share.version=1` in its latest stable
feature mapping. Its shipped single-node `server.properties` also sets
`share.coordinator.state.topic.replication.factor=1` and
`share.coordinator.state.topic.min.isr=1`.

The project will therefore remove explicit configuration for:

- `group.share.enable`
- `share.version=1` feature upgrades
- Share coordinator state-topic replication factor
- Share coordinator state-topic minimum ISR

These values will not be restored unless runtime verification shows that the
official image or Testcontainers changes those effective defaults.

## Compose Design

Both Kafka services will use `apache/kafka:4.3.0`.

The broker service will retain the listener, KRaft, offsets-topic,
transaction-topic, and initial rebalance settings needed by the current local
single-node topology. The share-specific environment variables and
feature-upgrade healthcheck will be removed.

The replacement healthcheck will only verify broker readiness with a standard
Kafka CLI request against `localhost:9092`. It will have no mutating side
effects.

The `kafka-init` one-shot service will remain because
`share.auto.offset.reset=earliest` is a dynamic share-group configuration, not
a broker image default. It will continue to run after the broker is healthy.

## Testcontainers Design

The integration-test container will use `apache/kafka:4.3.0` without
share-specific environment overrides.

Test setup will stop invoking `kafka-features.sh`. It will continue to:

1. Set `share.auto.offset.reset=earliest` on
   `playground.queue.workers`.
2. Create the queue and dead-letter topics.
3. Register the container bootstrap server with Spring.

This keeps Compose and Testcontainers behavior equivalent while avoiding a
shared provisioning abstraction for two short, environment-specific setup
paths.

## Documentation

`HELP.md` will identify Kafka 4.3.0 and explain that share groups are enabled
by the image's stable Kafka feature level. It will also note that the
application-specific earliest-offset policy is provisioned separately.

## Verification

Verification will proceed from narrow to broad:

1. Validate the resolved Compose model.
2. Run the Kafka integration tests against Testcontainers.
3. Run the complete Gradle test and Kotlin lint suite.
4. Start the Compose Kafka services and verify broker health plus successful
   `kafka-init` completion.
5. Inspect the effective share-group configuration and confirm
   `share.auto.offset.reset=earliest`.

If a removed setting is required in either runtime, only that demonstrated
setting will be restored, with a comment explaining why it differs from the
official Kafka 4.3 image defaults.
