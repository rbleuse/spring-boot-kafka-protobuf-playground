package io.github.rbleuse.playground.kafka

import io.github.rbleuse.playground.controller.PublishMessageRequest
import io.github.rbleuse.playground.domain.QueueMessageMapper
import io.github.rbleuse.playground.proto.QueueMessages
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import java.util.function.BiConsumer

internal fun logPublishFailure(
	logger: Logger,
	messageId: String,
) = BiConsumer<SendResult<String, QueueMessages.QueueMessage>?, Throwable?> { _, exception ->
	if (exception != null) {
		logger.error("Failed to publish queue message id={}", messageId, exception)
	}
}

@Component
class MessagePublisher(
	private val kafkaTemplate: KafkaTemplate<String, QueueMessages.QueueMessage>,
	private val mapper: QueueMessageMapper,
	@Value("\${playground.kafka.topic}") private val topic: String,
) {

	fun publish(request: PublishMessageRequest) {
		val message = mapper.toProto(request)
		kafkaTemplate.send(topic, message.id, message)
			.whenComplete(logPublishFailure(logger, message.id))
	}

	private companion object {
		val logger = LoggerFactory.getLogger(MessagePublisher::class.java)
	}
}
