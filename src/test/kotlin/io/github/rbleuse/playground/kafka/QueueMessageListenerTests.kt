package io.github.rbleuse.playground.kafka

import io.github.rbleuse.playground.proto.QueueMessages
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldBeEmpty
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer

class QueueMessageListenerTests {
    private val processedMessages = ProcessedMessages()
    private val listener = QueueMessageListener(processedMessages)

    @Test
    fun `ignores tombstone`() {
        listener.listen(record(null))

        processedMessages.all().shouldBeEmpty()
    }

    @Test
    fun `throws deserialization failure carried by null record`() {
        val headers = RecordHeaders()
        val deserializer = ErrorHandlingDeserializer(QueueMessageDeserializer())
        deserializer.deserialize("playground.queue", headers, byteArrayOf(0x80.toByte()))

        shouldThrow<DeserializationException> {
            listener.listen(record(null, headers))
        }
        processedMessages.all().shouldBeEmpty()
    }

    private fun record(
        value: QueueMessages.QueueMessage?,
        headers: RecordHeaders = RecordHeaders(),
    ) = ConsumerRecord(
        "playground.queue",
        0,
        0,
        0,
        TimestampType.CREATE_TIME,
        0,
        0,
        "key",
        value,
        headers,
        null,
    )
}
