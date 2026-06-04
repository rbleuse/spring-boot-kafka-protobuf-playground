package io.github.rbleuse.playground.config

import io.github.rbleuse.playground.kafka.QueueMessageDeserializer
import io.github.rbleuse.playground.kafka.QueueMessageSerializer
import io.github.rbleuse.playground.proto.QueueMessages
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.AcknowledgeType
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.log.LogAccessor
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.DefaultShareConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.core.ShareConsumerFactory
import org.springframework.kafka.support.KafkaUtils
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.SerializationUtils

@Configuration
@EnableKafka
class KafkaQueueConfiguration(
    private val kafkaProperties: KafkaProperties,
) {
    @Bean
    fun producerFactory(): ProducerFactory<String, QueueMessages.QueueMessage> {
        val properties = kafkaProperties.buildProducerProperties()
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = QueueMessageSerializer::class.java
        return DefaultKafkaProducerFactory(properties)
    }

    @Bean
    fun kafkaTemplate(producerFactory: ProducerFactory<String, QueueMessages.QueueMessage>) = KafkaTemplate(producerFactory)

    @Bean
    fun byteProducerFactory(): ProducerFactory<String, ByteArray> {
        val properties = kafkaProperties.buildProducerProperties()
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        return DefaultKafkaProducerFactory(properties)
    }

    @Bean
    fun byteKafkaTemplate(byteProducerFactory: ProducerFactory<String, ByteArray>) = KafkaTemplate(byteProducerFactory)

    @Bean
    fun queueTopics(
        @Value("\${playground.kafka.topic}") topic: String,
        @Value("\${playground.kafka.dlt-topic}") dltTopic: String,
    ) = KafkaAdmin.NewTopics(
        NewTopic(topic, 1, 1.toShort()),
        NewTopic(dltTopic, 1, 1.toShort()),
    )

    @Bean
    fun shareConsumerFactory(): ShareConsumerFactory<String, QueueMessages.QueueMessage> {
        val properties = kafkaProperties.buildConsumerProperties()
        properties.remove(ConsumerConfig.ISOLATION_LEVEL_CONFIG)
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ErrorHandlingDeserializer::class.java
        properties[ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS] = QueueMessageDeserializer::class.java.name
        return DefaultShareConsumerFactory(properties)
    }

    @Bean
    fun shareKafkaListenerContainerFactory(
        shareConsumerFactory: ShareConsumerFactory<String, QueueMessages.QueueMessage>,
        byteKafkaTemplate: KafkaTemplate<String, ByteArray>,
        @Value("\${playground.kafka.dlt-topic}") dltTopic: String,
    ) = ShareKafkaListenerContainerFactory(shareConsumerFactory).apply {
        setShareConsumerRecordRecoverer { record, _ ->
            try {
                byteKafkaTemplate.send(rejectedRecord(dltTopic, record)).get()
            } catch (exception: Exception) {
                logAccessor.error(exception) { "Failed to publish rejected queue message to $dltTopic" }
            }
            AcknowledgeType.REJECT
        }
    }

    private fun rejectedRecord(
        dltTopic: String,
        record: ConsumerRecord<*, *>,
    ) = ProducerRecord<String, ByteArray>(
        dltTopic,
        record.partition(),
        record.timestamp(),
        record.key()?.toString(),
        originalBytes(record),
        record.headers(),
    )

    private fun originalBytes(record: ConsumerRecord<*, *>): ByteArray =
        SerializationUtils
            .getExceptionFromHeader(
                record,
                KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER,
                logAccessor,
            )?.data ?: error("Missing value deserialization failure header")

    private companion object {
        val logAccessor = LogAccessor(KafkaQueueConfiguration::class.java)
    }
}
