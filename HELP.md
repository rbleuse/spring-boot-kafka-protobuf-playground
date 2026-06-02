# Kafka Queue Protobuf Playground

Run the app with:

```powershell
.\gradlew.bat bootRun
```

Spring Boot Compose support starts `apache/kafka:4.2.1` from `compose.yaml`.
The broker healthcheck enables the Kafka queue feature `share.version=1` before
the app connects.

Publish an email:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/messages `
  -ContentType application/json `
  -Body '{"type":"email","recipient":"demo@example.com","subject":"Hello"}'
```

Supported types are `email`, `sms`, `push`, and `audit`. Valid records are
processed from `playground.queue`. Malformed protobuf records are rejected to
`playground.queue.DLT`.
