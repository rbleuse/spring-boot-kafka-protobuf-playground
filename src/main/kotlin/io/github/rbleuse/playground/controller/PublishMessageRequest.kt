package io.github.rbleuse.playground.controller

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    JsonSubTypes.Type(value = EmailRequest::class, name = "email"),
    JsonSubTypes.Type(value = SmsRequest::class, name = "sms"),
    JsonSubTypes.Type(value = PushRequest::class, name = "push"),
    JsonSubTypes.Type(value = AuditRequest::class, name = "audit"),
)
sealed interface PublishMessageRequest

data class EmailRequest(
    val recipient: String,
    val subject: String,
) : PublishMessageRequest

data class SmsRequest(
    val phoneNumber: String,
    val text: String,
) : PublishMessageRequest

data class PushRequest(
    val deviceToken: String,
    val title: String,
) : PublishMessageRequest

data class AuditRequest(
    val actor: String,
    val action: String,
) : PublishMessageRequest
