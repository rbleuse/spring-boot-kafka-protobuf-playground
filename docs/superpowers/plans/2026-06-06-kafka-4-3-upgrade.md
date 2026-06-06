# Kafka 4.3 Upgrade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade Compose and Testcontainers to Apache Kafka 4.3.0 while removing share-group configuration already supplied by the official single-node image.

**Architecture:** Both runtime paths will rely on Kafka 4.3's stable `share.version=1` feature level and shipped single-node share-state topic settings. The application-specific `share.auto.offset.reset=earliest` group configuration remains explicit in Compose initialization and Testcontainers setup.

**Tech Stack:** Apache Kafka 4.3.0, Docker Compose, Testcontainers Kafka, Spring Boot, Kotlin, Gradle

---

### Task 1: Simplify the Compose Kafka Runtime

**Files:**
- Modify: `compose.yaml`

- [ ] **Step 1: Record the current Compose configuration**

Run:

```powershell
docker compose config
```

Expected: the resolved model contains `apache/kafka:4.2.1`, the three
share-specific environment variables, and the feature-upgrade healthcheck.

- [ ] **Step 2: Upgrade images and remove redundant share configuration**

Change both image references to:

```yaml
image: apache/kafka:4.3.0
```

Remove these broker environment entries:

```yaml
KAFKA_GROUP_SHARE_ENABLE: "true"
KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 1
KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: 1
```

Replace the mutating healthcheck with:

```yaml
healthcheck:
  test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
  interval: 2s
  timeout: 10s
  retries: 30
  start_period: 5s
```

Keep the `kafka-init` command that applies:

```text
share.auto.offset.reset=earliest
```

- [ ] **Step 3: Validate the resolved Compose model**

Run:

```powershell
docker compose config
```

Expected: success; both services use `apache/kafka:4.3.0`; no
`KAFKA_GROUP_SHARE_ENABLE`, share coordinator state-topic override, or
`kafka-features.sh` command remains.

### Task 2: Simplify the Testcontainers Kafka Fixture

**Files:**
- Modify: `src/test/kotlin/io/github/rbleuse/playground/kafka/KafkaQueueIntegrationTests.kt`

- [ ] **Step 1: Run the existing Kafka integration test as a baseline**

Run:

```powershell
.\gradlew.bat test --tests "io.github.rbleuse.playground.kafka.KafkaQueueIntegrationTests"
```

Expected: PASS against Kafka 4.2.1.

- [ ] **Step 2: Upgrade the image and remove redundant setup**

Replace the container declaration with:

```kotlin
KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"))
```

Remove the three `.withEnv(...)` calls and remove the complete
`kafka-features.sh` `execInContainer` call. Keep the
`kafka-configs.sh` command for `share.auto.offset.reset=earliest` and topic
creation through `AdminClient`.

- [ ] **Step 3: Run the Kafka integration test**

Run:

```powershell
.\gradlew.bat test --tests "io.github.rbleuse.playground.kafka.KafkaQueueIntegrationTests"
```

Expected: PASS, proving Kafka 4.3 starts with share groups enabled and the
single-node share-state defaults are sufficient.

- [ ] **Step 4: Restore only evidence-based configuration if needed**

If the test fails due to the `__share_group_state` replication or ISR settings,
restore only the failing property as a `.withEnv(...)` entry and document why.
If share groups are unavailable, inspect the active feature levels before
restoring any feature-upgrade command.

### Task 3: Update Runtime Documentation

**Files:**
- Modify: `HELP.md`

- [ ] **Step 1: Update the documented Kafka version and provisioning behavior**

Replace the Kafka startup description with:

```markdown
Spring Boot Compose support starts `apache/kafka:4.3.0` from `compose.yaml`.
Kafka 4.3 enables the stable share-group feature in fresh clusters, while a
one-shot Compose service configures `playground.queue.workers` to start at the
earliest available record.
```

- [ ] **Step 2: Check for stale Kafka 4.2 references**

Run:

```powershell
rg "4\.2\.1|KAFKA_GROUP_SHARE_ENABLE|share\.version=1|KAFKA_SHARE_COORDINATOR" .
```

Expected: no matches outside historical design or plan documentation.

### Task 4: Verify the Complete Upgrade

**Files:**
- No production file changes expected

- [ ] **Step 1: Run lint and all tests**

Run:

```powershell
.\gradlew.bat lintKotlin test
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 2: Start the Compose services**

Run:

```powershell
docker compose up -d --wait kafka kafka-init
```

Expected: Kafka becomes healthy and `kafka-init` exits with code `0`.

- [ ] **Step 3: Verify service state**

Run:

```powershell
docker compose ps -a
```

Expected: `kafka` is healthy and `kafka-init` is exited with code `0`.

- [ ] **Step 4: Verify the share-group dynamic configuration**

Run:

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type groups --entity-name playground.queue.workers
```

Expected: output contains:

```text
share.auto.offset.reset=earliest
```

- [ ] **Step 5: Verify active Kafka features**

Run:

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-features.sh --bootstrap-controller localhost:9093 describe
```

Expected: output reports `share.version` at level `1`.

- [ ] **Step 6: Stop the verification stack**

Run:

```powershell
docker compose down
```

Expected: Compose services and network are removed successfully.

- [ ] **Step 7: Review the final diff**

Run:

```powershell
git diff --check
git diff -- compose.yaml HELP.md src/test/kotlin/io/github/rbleuse/playground/kafka/KafkaQueueIntegrationTests.kt
```

Expected: no whitespace errors; the diff is limited to the Kafka 4.3 upgrade,
share configuration simplification, and documentation update.
