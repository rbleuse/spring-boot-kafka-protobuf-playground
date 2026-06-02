package io.github.rbleuse.playground.domain

import io.github.rbleuse.playground.controller.AuditRequest
import io.github.rbleuse.playground.controller.EmailRequest
import io.github.rbleuse.playground.controller.PushRequest
import io.github.rbleuse.playground.controller.SmsRequest
import io.github.rbleuse.playground.proto.QueueMessages.QueueMessage.PayloadCase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class QueueMessageMapperTests {

	private val mapper = QueueMessageMapper()

	@Test
	fun `maps email request to email payload`() {
		val message = mapper.toProto(EmailRequest(recipient = "person@example.com", subject = "Hello"))

		UUID.fromString(message.id)
		assertEquals(PayloadCase.EMAIL, message.payloadCase)
		assertEquals("person@example.com", message.email.recipient)
		assertEquals("Hello", message.email.subject)
	}

	@Test
	fun `maps sms request to sms payload`() {
		val message = mapper.toProto(SmsRequest(phoneNumber = "+15551234567", text = "On my way"))

		UUID.fromString(message.id)
		assertEquals(PayloadCase.SMS, message.payloadCase)
		assertEquals("+15551234567", message.sms.phoneNumber)
		assertEquals("On my way", message.sms.text)
	}

	@Test
	fun `maps push request to push payload`() {
		val message = mapper.toProto(PushRequest(deviceToken = "device-token", title = "New message"))

		UUID.fromString(message.id)
		assertEquals(PayloadCase.PUSH, message.payloadCase)
		assertEquals("device-token", message.push.deviceToken)
		assertEquals("New message", message.push.title)
	}

	@Test
	fun `maps audit request to audit payload`() {
		val message = mapper.toProto(AuditRequest(actor = "admin", action = "user-disabled"))

		UUID.fromString(message.id)
		assertEquals(PayloadCase.AUDIT, message.payloadCase)
		assertEquals("admin", message.audit.actor)
		assertEquals("user-disabled", message.audit.action)
	}
}
