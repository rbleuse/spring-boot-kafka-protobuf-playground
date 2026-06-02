package io.github.rbleuse.playground.domain

import io.github.rbleuse.playground.controller.AuditRequest
import io.github.rbleuse.playground.controller.EmailRequest
import io.github.rbleuse.playground.controller.PushRequest
import io.github.rbleuse.playground.controller.SmsRequest
import io.github.rbleuse.playground.proto.QueueMessages.QueueMessage.PayloadCase
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import java.util.UUID

class QueueMessageMapperTests {

	private val mapper = QueueMessageMapper()

	@Test
	fun `maps email request to email payload`() {
		val message = mapper.toProto(EmailRequest(recipient = "person@example.com", subject = "Hello"))

		UUID.fromString(message.id)
		message.payloadCase shouldBe PayloadCase.EMAIL
		message.email.recipient shouldBe "person@example.com"
		message.email.subject shouldBe "Hello"
	}

	@Test
	fun `maps sms request to sms payload`() {
		val message = mapper.toProto(SmsRequest(phoneNumber = "+15551234567", text = "On my way"))

		UUID.fromString(message.id)
		message.payloadCase shouldBe PayloadCase.SMS
		message.sms.phoneNumber shouldBe "+15551234567"
		message.sms.text shouldBe "On my way"
	}

	@Test
	fun `maps push request to push payload`() {
		val message = mapper.toProto(PushRequest(deviceToken = "device-token", title = "New message"))

		UUID.fromString(message.id)
		message.payloadCase shouldBe PayloadCase.PUSH
		message.push.deviceToken shouldBe "device-token"
		message.push.title shouldBe "New message"
	}

	@Test
	fun `maps audit request to audit payload`() {
		val message = mapper.toProto(AuditRequest(actor = "admin", action = "user-disabled"))

		UUID.fromString(message.id)
		message.payloadCase shouldBe PayloadCase.AUDIT
		message.audit.actor shouldBe "admin"
		message.audit.action shouldBe "user-disabled"
	}
}
