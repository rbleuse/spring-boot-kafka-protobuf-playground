package io.github.rbleuse.playground.kafka

import io.github.rbleuse.playground.proto.QueueMessages
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.core.log.LogAccessor
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaUtils
import org.springframework.kafka.support.serializer.SerializationUtils
import org.springframework.stereotype.Component

@Component
class QueueMessageListener(
    private val processedMessages: ProcessedMessages,
) {
    @KafkaListener(
        topics = ["\${playground.kafka.topic}"],
        groupId = "\${playground.kafka.share-group}",
        containerFactory = "shareKafkaListenerContainerFactory",
        autoStartup = "\${playground.kafka.listener-auto-startup}",
    )
    fun listen(record: ConsumerRecord<String, QueueMessages.QueueMessage?>) {
        val message = record.value()
        if (message == null) {
            val exception =
                SerializationUtils.getExceptionFromHeader(
                    record,
                    KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER,
                    logAccessor,
                )
            if (exception != null) {
                throw exception
            }

            logger.info("Ignoring queue tombstone key={}", record.key())
            return
        }

        logger.info("Processed queue message id={} payloadCase={}", message.id, message.payloadCase)
        processedMessages.add(message)
    }

    private companion object {
        val logger = LoggerFactory.getLogger(QueueMessageListener::class.java)
        val logAccessor = LogAccessor(QueueMessageListener::class.java)
    }
}
