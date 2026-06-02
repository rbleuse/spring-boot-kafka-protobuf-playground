package io.github.rbleuse.playground.kafka

import io.github.rbleuse.playground.proto.QueueMessages
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class QueueMessageListener(
    private val processedMessages: ProcessedMessages,
) {
    @KafkaListener(
        topics = ["\${playground.kafka.topic}"],
        groupId = "\${playground.kafka.share-group}",
        containerFactory = "shareKafkaListenerContainerFactory",
    )
    fun listen(message: QueueMessages.QueueMessage) {
        logger.info("Processed queue message id={} payloadCase={}", message.id, message.payloadCase)
        processedMessages.add(message)
    }

    private companion object {
        val logger = LoggerFactory.getLogger(QueueMessageListener::class.java)
    }
}
