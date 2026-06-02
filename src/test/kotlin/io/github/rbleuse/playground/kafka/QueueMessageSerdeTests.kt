package io.github.rbleuse.playground.kafka

import io.github.rbleuse.playground.proto.QueueMessages
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.apache.kafka.common.errors.SerializationException
import org.junit.jupiter.api.Test

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
