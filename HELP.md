# Kafka Queue Protobuf Playground

Run the app with:

```powershell
.\gradlew.bat bootRun
```

Spring Boot Compose support starts `apache/kafka:4.4.0` from `compose.yaml`.
Kafka 4.4 enables the stable share-group feature in fresh clusters.
`share.auto.offset.reset` remains a group-level setting with a default of
`latest`, so `kafka-init` configures `playground.queue.workers` to start at the
earliest available record. The single-node Compose and Testcontainers brokers
also reduce the internal share-state topic replication factor and minimum ISR
to one.

Publish an email:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/messages `
  -ContentType application/json `
  -Body '{"type":"email","recipient":"demo@example.com","subject":"Hello"}'
```

Supported types are `email`, `sms`, `push`, and `audit`. Valid records are
processed from `playground.queue`. Malformed protobuf records are rejected to
`playground.queue.DLT`.
