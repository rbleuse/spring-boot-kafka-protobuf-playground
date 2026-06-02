# Kafka Queue Protobuf Playground Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a minimal Spring Boot Kotlin playground that publishes polymorphic protobuf messages over HTTP, consumes them with a Kafka 4.2 share-group listener, and rejects malformed protobuf records to a DLT.

**Architecture:** A sealed JSON request hierarchy maps to a generated protobuf `QueueMessage` with a `oneof` payload. A standard `KafkaTemplate<String, QueueMessage>` publishes records, while a Spring Kafka `ShareKafkaListenerContainerFactory` consumes them with a custom protobuf deserializer wrapped by `ErrorHandlingDeserializer`. Deserialization failures are recovered to `playground.queue.DLT` and rejected so later records continue processing.

**Tech Stack:** Spring Boot 4.1.0-RC1, Kotlin 2.3.21, Spring Kafka 4.1.x, Apache Kafka 4.2.1, protobuf Java runtime and compiler 4.35.0, protobuf Gradle plugin 0.10.0, Spring Boot-managed Testcontainers, Kotest 6.1.x assertions, Gradle 9.5.1, Java 25.

---

## File Structure

Keep the root application class in `io.github.rbleuse.playground` so component
scanning covers these focused packages:

- `controller` - HTTP transport types and controller
- `domain` - request-to-protobuf mapping
- `kafka` - Kafka adapters, listener, serde, publisher, and the demo observer
- `config` - Spring Kafka wiring
- `proto` - generated protobuf Java classes

- Modify: `build.gradle.kts` - add web, Kafka, protobuf, Compose, Testcontainers, and Kotest dependencies plus protobuf generation.
- Create: `compose.yaml` - run a single Apache Kafka 4.2.1 broker for local development.
- Modify: `src/main/resources/application.yaml` - keep topic and share-group names in one small configuration block.
- Create: `src/main/proto/queue_message.proto` - define the protobuf envelope and four `oneof` payload variants.
- Create: `src/main/kotlin/io/github/rbleuse/playground/controller/PublishMessageRequest.kt` - define the Jackson-polymorphic HTTP request hierarchy.
- Create: `src/main/kotlin/io/github/rbleuse/playground/controller/MessageController.kt` - implement `POST /messages`.
- Create: `src/main/kotlin/io/github/rbleuse/playground/domain/QueueMessageMapper.kt` - map HTTP requests to generated protobuf messages.
- Create: `src/main/kotlin/io/github/rbleuse/playground/kafka/QueueMessageSerde.kt` - serialize and deserialize generated protobuf messages.
- Create: `src/main/kotlin/io/github/rbleuse/playground/kafka/ProcessedMessages.kt` - expose the listener's in-memory observation point for tests.
- Create: `src/main/kotlin/io/github/rbleuse/playground/kafka/MessagePublisher.kt` - publish protobuf messages.
- Create: `src/main/kotlin/io/github/rbleuse/playground/kafka/QueueMessageListener.kt` - process valid share-group records.
- Create: `src/main/kotlin/io/github/rbleuse/playground/config/KafkaQueueConfiguration.kt` - configure topics, producer, share consumer, and malformed-record recovery.
- Replace: `src/test/kotlin/io/github/rbleuse/playground/SpringBootKafkaProtobuffApplicationTests.kt` - add Testcontainers-backed end-to-end coverage.

### Task 1: Add Dependencies And Generate The Protobuf Envelope

**Files:**
- Modify: `build.gradle.kts`
- Create: `src/main/proto/queue_message.proto`

- [ ] **Step 1: Add protobuf generation and runtime dependencies**

Add the protobuf Gradle plugin, Java support for generated sources, and the application dependencies:

```kotlin
plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.spring") version "2.3.21"
    id("org.springframework.boot") version "4.1.0-RC1"
    id("io.spring.dependency-management") version "1.1.7"
    id("com.google.protobuf") version "0.10.0"
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.google.protobuf:protobuf-java:4.35.0")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    developmentOnly("org.springframework.boot:spring-boot-docker-compose")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    // Version comes from Spring Boot's imported dependency-management BOM.
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("io.kotest:kotest-assertions-core-jvm:6.1.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.35.0"
    }
}
```

- [ ] **Step 2: Define the protobuf model**

Create `src/main/proto/queue_message.proto`:

```proto
syntax = "proto3";

package playground;

option java_package = "io.github.rbleuse.playground.proto";
option java_outer_classname = "QueueMessages";

message QueueMessage {
  string id = 1;

  oneof payload {
    Email email = 2;
    Sms sms = 3;
    Push push = 4;
    Audit audit = 5;
  }
}

message Email {
  string recipient = 1;
  string subject = 2;
}

message Sms {
  string phone_number = 1;
  string text = 2;
}

message Push {
  string device_token = 1;
  string title = 2;
}

message Audit {
  string actor = 1;
  string action = 2;
}
```

- [ ] **Step 3: Run protobuf generation**

Run: `.\gradlew.bat generateProto`

Expected: `BUILD SUCCESSFUL` and generated `QueueMessages.java` under `build/generated/sources/proto/main/java`.

- [ ] **Step 4: Commit**

```bash
git add build.gradle.kts src/main/proto/queue_message.proto
git commit -m "build: add protobuf and kafka dependencies"
```

### Task 2: Map Polymorphic JSON Requests To Protobuf

**Files:**
- Create: `src/main/kotlin/io/github/rbleuse/playground/controller/PublishMessageRequest.kt`
- Create: `src/main/kotlin/io/github/rbleuse/playground/domain/QueueMessageMapper.kt`
- Create: `src/test/kotlin/io/github/rbleuse/playground/domain/QueueMessageMapperTests.kt`

- [ ] **Step 1: Write the mapper tests**

Create `QueueMessageMapperTests.kt` with one test per variant:

```kotlin
class QueueMessageMapperTests {
    private val mapper = QueueMessageMapper()

    @Test
    fun `maps email request`() {
        val message = mapper.toProto(EmailRequest("demo@example.com", "Hello"))
        message.payloadCase shouldBe QueueMessages.QueueMessage.PayloadCase.EMAIL
        message.email.recipient shouldBe "demo@example.com"
    }

    @Test
    fun `maps sms request`() {
        mapper.toProto(SmsRequest("+33123456789", "Hello")).sms.text shouldBe "Hello"
    }

    @Test
    fun `maps push request`() {
        mapper.toProto(PushRequest("device-1", "Hello")).push.deviceToken shouldBe "device-1"
    }

    @Test
    fun `maps audit request`() {
        mapper.toProto(AuditRequest("demo", "login")).audit.action shouldBe "login"
    }
}
```

- [ ] **Step 2: Run the mapper tests to verify they fail**

Run: `.\gradlew.bat test --tests "*QueueMessageMapperTests"`

Expected: compilation fails because the request types and mapper do not exist.

- [ ] **Step 3: Implement the sealed request hierarchy**

Create `controller/PublishMessageRequest.kt`:

```kotlin
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(EmailRequest::class, name = "email"),
    JsonSubTypes.Type(SmsRequest::class, name = "sms"),
    JsonSubTypes.Type(PushRequest::class, name = "push"),
    JsonSubTypes.Type(AuditRequest::class, name = "audit"),
)
sealed interface PublishMessageRequest

data class EmailRequest(val recipient: String, val subject: String) : PublishMessageRequest
data class SmsRequest(val phoneNumber: String, val text: String) : PublishMessageRequest
data class PushRequest(val deviceToken: String, val title: String) : PublishMessageRequest
data class AuditRequest(val actor: String, val action: String) : PublishMessageRequest
```

- [ ] **Step 4: Implement the mapper**

Create `domain/QueueMessageMapper.kt` with `@Component` and a `toProto(request)` function. Build `QueueMessages.QueueMessage`, assign `UUID.randomUUID().toString()` to `id`, and set exactly one generated payload builder in an exhaustive `when`.

- [ ] **Step 5: Run the mapper tests**

Run: `.\gradlew.bat test --tests "*QueueMessageMapperTests"`

Expected: all four tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/io/github/rbleuse/playground/controller src/main/kotlin/io/github/rbleuse/playground/domain src/test/kotlin/io/github/rbleuse/playground/domain
git commit -m "feat: map polymorphic requests to protobuf"
```

### Task 3: Add Protobuf Serde And A Share-Group Listener

**Files:**
- Create: `src/main/kotlin/io/github/rbleuse/playground/kafka/QueueMessageSerde.kt`
- Create: `src/main/kotlin/io/github/rbleuse/playground/kafka/ProcessedMessages.kt`
- Create: `src/main/kotlin/io/github/rbleuse/playground/config/KafkaQueueConfiguration.kt`
- Create: `src/main/kotlin/io/github/rbleuse/playground/kafka/QueueMessageListener.kt`
- Modify: `src/main/resources/application.yaml`
- Create: `src/test/kotlin/io/github/rbleuse/playground/kafka/QueueMessageSerdeTests.kt`

- [ ] **Step 1: Write serde tests**

Create `QueueMessageSerdeTests.kt`:

```kotlin
class QueueMessageSerdeTests {
    private val serializer = QueueMessageSerializer()
    private val deserializer = QueueMessageDeserializer()

    @Test
    fun `round trips protobuf message`() {
        val message = QueueMessages.QueueMessage.newBuilder()
            .setId("message-1")
            .setAudit(QueueMessages.Audit.newBuilder().setActor("demo").setAction("login"))
            .build()

        deserializer.deserialize("playground.queue", serializer.serialize("playground.queue", message)) shouldBe message
    }

    @Test
    fun `rejects malformed protobuf bytes`() {
        shouldThrow<SerializationException> {
            deserializer.deserialize("playground.queue", byteArrayOf(0x80.toByte()))
        }
    }
}
```

- [ ] **Step 2: Run the serde tests to verify they fail**

Run: `.\gradlew.bat test --tests "*QueueMessageSerdeTests"`

Expected: compilation fails because the serde classes do not exist.

- [ ] **Step 3: Implement protobuf serde**

Create `kafka/QueueMessageSerde.kt`. Implement Kafka `Serializer<QueueMessages.QueueMessage>` with `data?.toByteArray()` and Kafka `Deserializer<QueueMessages.QueueMessage>` with `QueueMessages.QueueMessage.parseFrom(data)`. Wrap `InvalidProtocolBufferException` in Kafka `SerializationException`.

- [ ] **Step 4: Add observable processing**

Create `kafka/ProcessedMessages.kt` as a `@Component` backed by a thread-safe `CopyOnWriteArrayList<QueueMessages.QueueMessage>`. Expose `add`, `clear`, and `all` methods.

- [ ] **Step 5: Configure the share consumer**

Create `config/KafkaQueueConfiguration.kt`:

```kotlin
@Configuration
@EnableKafka
class KafkaQueueConfiguration(
    private val kafkaProperties: KafkaProperties,
) {
    @Bean
    fun shareConsumerFactory(): ShareConsumerFactory<String, QueueMessages.QueueMessage> {
        val properties = kafkaProperties.buildConsumerProperties(null)
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ErrorHandlingDeserializer::class.java
        properties[ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS] = QueueMessageDeserializer::class.java.name
        return DefaultShareConsumerFactory(properties)
    }

    @Bean
    fun shareKafkaListenerContainerFactory(
        shareConsumerFactory: ShareConsumerFactory<String, QueueMessages.QueueMessage>,
    ) = ShareKafkaListenerContainerFactory(shareConsumerFactory)
}
```

Add `playground.kafka.topic`, `playground.kafka.dlt-topic`, and `playground.kafka.share-group` properties to `application.yaml`, all using the names in the approved spec.

- [ ] **Step 6: Add the valid-message listener**

Create `kafka/QueueMessageListener.kt` with a `@KafkaListener` using `containerFactory = "shareKafkaListenerContainerFactory"` and the configured topic and share group. Its listener method adds valid `QueueMessage` records to `ProcessedMessages` and logs their ids and payload cases.

- [ ] **Step 7: Run tests**

Run: `.\gradlew.bat test --tests "*QueueMessageSerdeTests" --tests "*QueueMessageMapperTests"`

Expected: mapper and serde tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/main/kotlin/io/github/rbleuse/playground/config src/main/kotlin/io/github/rbleuse/playground/kafka src/main/resources/application.yaml src/test/kotlin/io/github/rbleuse/playground/kafka
git commit -m "feat: consume protobuf records with kafka queues"
```

### Task 4: Publish HTTP Requests

**Files:**
- Create: `src/main/kotlin/io/github/rbleuse/playground/kafka/MessagePublisher.kt`
- Create: `src/main/kotlin/io/github/rbleuse/playground/controller/MessageController.kt`
- Create: `src/test/kotlin/io/github/rbleuse/playground/controller/MessageControllerTests.kt`

- [ ] **Step 1: Write the HTTP slice test**

Create a `@WebMvcTest(MessageController::class)` test with mocked `MessagePublisher`. POST this body:

```json
{"type":"email","recipient":"demo@example.com","subject":"Hello"}
```

Assert status `202 Accepted` and verify the publisher receives an `EmailRequest`.

- [ ] **Step 2: Run the controller test to verify it fails**

Run: `.\gradlew.bat test --tests "*MessageControllerTests"`

Expected: compilation fails because the publisher and controller do not exist.

- [ ] **Step 3: Implement publication**

Create `kafka/MessagePublisher.kt` as a `@Component`. Inject `KafkaTemplate<String, QueueMessages.QueueMessage>`, `QueueMessageMapper`, and `@Value("\${playground.kafka.topic}") topic`. Its `publish(request)` method maps and sends the message with its generated id as the key.

- [ ] **Step 4: Implement the endpoint**

Create `controller/MessageController.kt`:

```kotlin
@RestController
@RequestMapping("/messages")
class MessageController(private val publisher: MessagePublisher) {
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    fun publish(@RequestBody request: PublishMessageRequest) {
        publisher.publish(request)
    }
}
```

- [ ] **Step 5: Run the controller test**

Run: `.\gradlew.bat test --tests "*MessageControllerTests"`

Expected: the HTTP slice test passes.

- [ ] **Step 6: Commit**

```bash
git add src/main/kotlin/io/github/rbleuse/playground/controller src/main/kotlin/io/github/rbleuse/playground/kafka src/test/kotlin/io/github/rbleuse/playground/controller
git commit -m "feat: publish queue messages over http"
```

### Task 5: Reject Malformed Protobuf Records To The DLT

**Files:**
- Modify: `src/main/kotlin/io/github/rbleuse/playground/config/KafkaQueueConfiguration.kt`
- Create: `src/test/kotlin/io/github/rbleuse/playground/kafka/KafkaQueueIntegrationTests.kt`

- [ ] **Step 1: Start a Kafka 4.2.1 Testcontainer in the integration test**

Create `KafkaQueueIntegrationTests.kt` as a `@SpringBootTest(webEnvironment = RANDOM_PORT)` JUnit test. Use:

```kotlin
@Container
@JvmStatic
val kafka = KafkaContainer(DockerImageName.parse("apache/kafka:4.2.1"))

@DynamicPropertySource
@JvmStatic
fun kafkaProperties(registry: DynamicPropertyRegistry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers)
}
```

Use `TestRestTemplate`, `ProcessedMessages`, and a byte-array Kafka producer and consumer created from the container bootstrap servers.

- [ ] **Step 2: Write the failing happy-path integration test**

POST all four JSON variants to `/messages`, assert `202 Accepted`, and use Kotest `eventually(10.seconds)` until `processedMessages.all()` contains four records with payload cases `EMAIL`, `SMS`, `PUSH`, and `AUDIT`.

- [ ] **Step 3: Run the integration test**

Run: `.\gradlew.bat test --tests "*KafkaQueueIntegrationTests"`

Expected: valid processing passes once the share consumer configuration is wired correctly.

- [ ] **Step 4: Write the failing malformed-record integration test**

Send `byteArrayOf(0x80.toByte())` directly to `playground.queue`, then POST a valid audit JSON record. Poll `playground.queue.DLT` and assert it receives the original malformed byte array. Use `eventually(10.seconds)` to assert the later audit record is processed.

- [ ] **Step 5: Configure immediate DLT recovery and rejection**

Extend `config/KafkaQueueConfiguration.kt` with a byte-array `KafkaTemplate` for DLT publication and configure the share listener container to recover failed records by publishing the original bytes to `playground.queue.DLT` and returning `AcknowledgeType.REJECT`.

Use Spring Kafka's `ErrorHandlingDeserializer` header to retrieve the original malformed payload. Keep this logic in a small private helper. Do not retry malformed records.

- [ ] **Step 6: Run the integration test**

Run: `.\gradlew.bat test --tests "*KafkaQueueIntegrationTests"`

Expected: valid records are processed, malformed bytes appear on `playground.queue.DLT`, and the later valid record is processed.

- [ ] **Step 7: Commit**

```bash
git add src/main/kotlin/io/github/rbleuse/playground/config/KafkaQueueConfiguration.kt src/test/kotlin/io/github/rbleuse/playground/kafka/KafkaQueueIntegrationTests.kt
git commit -m "feat: reject malformed protobuf records to dlt"
```

### Task 6: Add Local Docker Compose Support

**Files:**
- Create: `compose.yaml`
- Modify: `HELP.md`

- [ ] **Step 1: Add the local broker**

Create `compose.yaml`:

```yaml
services:
  kafka:
    image: apache/kafka:4.2.1
    ports:
      - "9092"
```

- [ ] **Step 2: Document the playground**

Replace generated `HELP.md` content with a short guide covering:

```markdown
# Kafka Queue Protobuf Playground

Run the app with:

```powershell
.\gradlew.bat bootRun
```

Spring Boot starts `apache/kafka:4.2.1` from `compose.yaml`.

Publish an email:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/messages `
  -ContentType application/json `
  -Body '{"type":"email","recipient":"demo@example.com","subject":"Hello"}'
```

Supported types are `email`, `sms`, `push`, and `audit`. Valid records are
processed from `playground.queue`. Malformed protobuf records are rejected to
`playground.queue.DLT`.
```

- [ ] **Step 3: Validate Compose configuration**

Run: `docker compose config`

Expected: one `kafka` service using `apache/kafka:4.2.1`.

- [ ] **Step 4: Commit**

```bash
git add compose.yaml HELP.md
git commit -m "docs: add local kafka compose workflow"
```

### Task 7: Verify The Complete Playground

**Files:**
- No file changes expected

- [ ] **Step 1: Run the full test suite**

Run: `.\gradlew.bat test`

Expected: all mapper, serde, controller, context, and Kafka Testcontainers integration tests pass.

- [ ] **Step 2: Start the local application**

Run: `.\gradlew.bat bootRun`

Expected: Spring Boot starts the Compose Kafka broker, creates the share listener, and serves HTTP on port 8080.

- [ ] **Step 3: Exercise the endpoint**

Run:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/messages `
  -ContentType application/json `
  -Body '{"type":"audit","actor":"demo","action":"manual-check"}'
```

Expected: HTTP `202 Accepted` and an application log entry showing the processed audit record.

- [ ] **Step 4: Stop the application and check the worktree**

Stop `bootRun`, then run: `git status --short`

Expected: no generated or temporary files are tracked or left unignored.
