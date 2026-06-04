package io.github.rbleuse.playground.controller

import io.github.rbleuse.playground.domain.QueueMessageMapper
import io.github.rbleuse.playground.kafka.MessagePublisher
import io.github.rbleuse.playground.kafka.logPublishFailure
import io.github.rbleuse.playground.proto.QueueMessages
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import java.util.concurrent.CompletableFuture

class MessageControllerTests {
    @Test
    fun `publishes email request and accepts submission`() {
        val publisher = mockk<MessagePublisher>(relaxed = true)
        val mockMvc = MockMvcBuilders.standaloneSetup(MessageController(publisher)).build()

        mockMvc
            .perform(
                post("/messages")
                    .contentType("application/json")
                    .content("""{"type":"email","recipient":"person@example.com","subject":"Hello"}"""),
            ).andExpect(status().isAccepted)

        verify { publisher.publish(EmailRequest(recipient = "person@example.com", subject = "Hello")) }
    }
}

class MessagePublisherTests {
    @Test
    fun `maps request and sends message id as kafka key`() {
        val kafkaTemplate = mockk<KafkaTemplate<String, QueueMessages.QueueMessage>>()
        val mapper = mockk<QueueMessageMapper>()
        val publisher = MessagePublisher(kafkaTemplate, mapper, "playground.queue")
        val request = EmailRequest(recipient = "person@example.com", subject = "Hello")
        val message =
            QueueMessages.QueueMessage
                .newBuilder()
                .setId("message-id")
                .build()
        every { mapper.toProto(request) } returns message
        every { kafkaTemplate.send("playground.queue", "message-id", message) } returns
            CompletableFuture.completedFuture(null)

        publisher.publish(request)

        verify { mapper.toProto(request) }
        verify { kafkaTemplate.send("playground.queue", "message-id", message) }
    }

    @Test
    fun `logs message id when kafka send fails asynchronously`() {
        val logger = mockk<Logger>(relaxed = true)
        val exception = IllegalStateException("broker unavailable")

        CompletableFuture
            .failedFuture<SendResult<String, QueueMessages.QueueMessage>>(exception)
            .whenComplete(logPublishFailure(logger, "message-id"))

        verify { logger.error("Failed to publish queue message id={}", "message-id", exception) }
    }
}
