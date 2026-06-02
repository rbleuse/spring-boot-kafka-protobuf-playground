package io.github.rbleuse.playground.domain

import io.github.rbleuse.playground.controller.AuditRequest
import io.github.rbleuse.playground.controller.EmailRequest
import io.github.rbleuse.playground.controller.PublishMessageRequest
import io.github.rbleuse.playground.controller.PushRequest
import io.github.rbleuse.playground.controller.SmsRequest
import io.github.rbleuse.playground.proto.QueueMessages
import org.springframework.stereotype.Component
import java.util.UUID

@Component
class QueueMessageMapper {

	fun map(request: PublishMessageRequest): QueueMessages.QueueMessage {
		val message = QueueMessages.QueueMessage.newBuilder()
			.setId(UUID.randomUUID().toString())

		return when (request) {
			is EmailRequest -> message.setEmail(
				QueueMessages.Email.newBuilder()
					.setRecipient(request.recipient)
					.setSubject(request.subject),
			)

			is SmsRequest -> message.setSms(
				QueueMessages.Sms.newBuilder()
					.setPhoneNumber(request.phoneNumber)
					.setText(request.text),
			)

			is PushRequest -> message.setPush(
				QueueMessages.Push.newBuilder()
					.setDeviceToken(request.deviceToken)
					.setTitle(request.title),
			)

			is AuditRequest -> message.setAudit(
				QueueMessages.Audit.newBuilder()
					.setActor(request.actor)
					.setAction(request.action),
			)
		}.build()
	}
}
