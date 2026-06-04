package io.github.rbleuse.playground.controller

import io.github.rbleuse.playground.kafka.MessagePublisher
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/messages")
class MessageController(
    private val publisher: MessagePublisher,
) {
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    fun publish(
        @RequestBody request: PublishMessageRequest,
    ) {
        publisher.publish(request)
    }
}
