package io.github.rbleuse.playground.kafka

import io.github.rbleuse.playground.proto.QueueMessages.QueueMessage.PayloadCase
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.shouldBe
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.kafka.support.KafkaUtils
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.web.client.RestClient
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine
import kotlin.time.Duration.Companion.seconds

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class KafkaQueueIntegrationTests {

    @org.springframework.beans.factory.annotation.Autowired
    private lateinit var processedMessages: ProcessedMessages

    @LocalServerPort
    private var port: Int = 0

    @BeforeEach
    fun clearProcessedMessages() {
        processedMessages.clear()
    }

    @Test
    fun `posts all message variants and processes them`() = runSuspending {
        post("""{"type":"email","recipient":"person@example.com","subject":"Hello"}""")
        post("""{"type":"sms","phoneNumber":"+15551234567","text":"On my way"}""")
        post("""{"type":"push","deviceToken":"device-token","title":"New message"}""")
        post("""{"type":"audit","actor":"admin","action":"user-disabled"}""")

        eventually(10.seconds) {
            processedMessages.all().map { it.payloadCase }
                .shouldContainExactlyInAnyOrder(PayloadCase.EMAIL, PayloadCase.SMS, PayloadCase.PUSH, PayloadCase.AUDIT)
        }
    }

    @Test
    fun `routes malformed bytes to dlt and processes a later valid audit`() = runSuspending {
        val malformed = byteArrayOf(0x80.toByte())
        byteProducer().use { producer ->
            producer.send(ProducerRecord(TOPIC, "malformed-key", malformed)).get()
        }

        post("""{"type":"audit","actor":"admin","action":"after-malformed"}""")

        byteConsumer().use { consumer ->
            consumer.subscribe(listOf(DLT_TOPIC))
            eventually(10.seconds) {
                val rejected = consumer.poll(Duration.ofMillis(250)).firstOrNull()
                rejected?.key() shouldBe "malformed-key"
                rejected?.value() shouldBe malformed
                (rejected?.headers()?.lastHeader(KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER) != null) shouldBe true
            }
        }
        eventually(10.seconds) {
            processedMessages.all().map { it.audit.action } shouldBe listOf("after-malformed")
        }
    }

    private fun post(json: String) {
        RestClient.create()
            .post()
            .uri("http://localhost:$port/messages")
            .contentType(MediaType.APPLICATION_JSON)
            .body(json)
            .retrieve()
            .toBodilessEntity()
            .statusCode shouldBe HttpStatus.ACCEPTED
    }

    private fun byteProducer() = KafkaProducer<String, ByteArray>(
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
        ),
    )

    private fun byteConsumer() = KafkaConsumer<String, ByteArray>(
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "dlt-${UUID.randomUUID()}",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
        ),
    )

    private companion object {
        const val TOPIC = "playground.queue"
        const val DLT_TOPIC = "playground.queue.DLT"

        @Container
        @JvmStatic
        val kafka = KafkaContainer(DockerImageName.parse("apache/kafka:4.2.1"))
            .withEnv("KAFKA_GROUP_SHARE_ENABLE", "true")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")

        @DynamicPropertySource
        @JvmStatic
        fun kafkaProperties(registry: DynamicPropertyRegistry) {
            val result = kafka.execInContainer(
                "/opt/kafka/bin/kafka-features.sh",
                "--bootstrap-controller",
                "localhost:9094",
                "upgrade",
                "--feature",
                "share.version=1",
            )
            check(result.exitCode == 0) { result.stderr }
            AdminClient.create(
                mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers),
            ).use { admin ->
                admin.createTopics(
                    listOf(
                        NewTopic(TOPIC, 1, 1.toShort()),
                        NewTopic(DLT_TOPIC, 1, 1.toShort()),
                    ),
                ).all().get()
            }
            registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers)
        }
    }
}

private fun runSuspending(block: suspend () -> Unit) {
    val completed = CountDownLatch(1)
    var outcome: Result<Unit>? = null
    block.startCoroutine(
        object : Continuation<Unit> {
            override val context = EmptyCoroutineContext

            override fun resumeWith(result: Result<Unit>) {
                outcome = result
                completed.countDown()
            }
        },
    )
    check(completed.await(30, TimeUnit.SECONDS)) { "Timed out waiting for test coroutine" }
    outcome!!.getOrThrow()
}
