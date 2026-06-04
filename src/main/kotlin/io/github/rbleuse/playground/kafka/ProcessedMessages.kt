package io.github.rbleuse.playground.kafka

import io.github.rbleuse.playground.proto.QueueMessages
import org.springframework.stereotype.Component
import java.util.concurrent.CopyOnWriteArrayList

@Component
class ProcessedMessages {
    private val messages = CopyOnWriteArrayList<QueueMessages.QueueMessage>()

    fun add(message: QueueMessages.QueueMessage) {
        messages.add(message)
    }

    fun clear() {
        messages.clear()
    }

    fun all(): List<QueueMessages.QueueMessage> = messages.toList()
}
