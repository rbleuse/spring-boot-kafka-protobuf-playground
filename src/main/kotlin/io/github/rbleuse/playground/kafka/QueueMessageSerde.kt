package io.github.rbleuse.playground.kafka

import com.google.protobuf.InvalidProtocolBufferException
import io.github.rbleuse.playground.proto.QueueMessages
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

class QueueMessageSerializer : Serializer<QueueMessages.QueueMessage> {
    override fun serialize(
        topic: String?,
        data: QueueMessages.QueueMessage?,
    ): ByteArray? = data?.toByteArray()
}

class QueueMessageDeserializer : Deserializer<QueueMessages.QueueMessage> {
    override fun deserialize(
        topic: String?,
        data: ByteArray?,
    ): QueueMessages.QueueMessage? {
        if (data == null) {
            return null
        }

        return try {
            QueueMessages.QueueMessage.parseFrom(data)
        } catch (exception: InvalidProtocolBufferException) {
            throw SerializationException("Could not deserialize QueueMessage protobuf", exception)
        }
    }
}
