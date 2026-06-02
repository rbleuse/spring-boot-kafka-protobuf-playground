package io.github.rbleuse.playground.kafka

import io.github.rbleuse.playground.controller.PublishMessageRequest
import io.github.rbleuse.playground.domain.QueueMessageMapper
import io.github.rbleuse.playground.proto.QueueMessages
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class MessagePublisher(
	private val kafkaTemplate: KafkaTemplate<String, QueueMessages.QueueMessage>,
	private val mapper: QueueMessageMapper,
	@Value("\${playground.kafka.topic}") private val topic: String,
) {

	fun publish(request: PublishMessageRequest) {
		val message = mapper.toProto(request)
		kafkaTemplate.send(topic, message.id, message)
	}
}
