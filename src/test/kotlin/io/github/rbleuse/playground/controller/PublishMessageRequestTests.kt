package io.github.rbleuse.playground.controller

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import tools.jackson.databind.ObjectMapper

class PublishMessageRequestTests {

	private val objectMapper = ObjectMapper()

	@Test
	fun `deserializes email discriminator`() {
		val request = read("""{"type":"email","recipient":"person@example.com","subject":"Hello"}""")

		request shouldBe EmailRequest(recipient = "person@example.com", subject = "Hello")
	}

	@Test
	fun `deserializes sms discriminator`() {
		val request = read("""{"type":"sms","phoneNumber":"+15551234567","text":"On my way"}""")

		request shouldBe SmsRequest(phoneNumber = "+15551234567", text = "On my way")
	}

	@Test
	fun `deserializes push discriminator`() {
		val request = read("""{"type":"push","deviceToken":"device-token","title":"New message"}""")

		request shouldBe PushRequest(deviceToken = "device-token", title = "New message")
	}

	@Test
	fun `deserializes audit discriminator`() {
		val request = read("""{"type":"audit","actor":"admin","action":"user-disabled"}""")

		request shouldBe AuditRequest(actor = "admin", action = "user-disabled")
	}

	@Test
	fun `rejects missing discriminator`() {
		shouldThrow<Exception> {
			read("""{"recipient":"person@example.com","subject":"Hello"}""")
		}
	}

	@Test
	fun `rejects unknown discriminator`() {
		shouldThrow<Exception> {
			read("""{"type":"other","value":"ignored"}""")
		}
	}

	private fun read(json: String): PublishMessageRequest =
		objectMapper.readValue(json, PublishMessageRequest::class.java)
}
