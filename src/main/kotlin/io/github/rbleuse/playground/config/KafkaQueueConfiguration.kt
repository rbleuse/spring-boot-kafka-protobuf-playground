package io.github.rbleuse.playground.config

import io.github.rbleuse.playground.kafka.QueueMessageDeserializer
import io.github.rbleuse.playground.kafka.QueueMessageSerializer
import io.github.rbleuse.playground.proto.QueueMessages
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.DefaultShareConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.core.ShareConsumerFactory
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer

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
    fun kafkaTemplate(
        producerFactory: ProducerFactory<String, QueueMessages.QueueMessage>,
    ) = KafkaTemplate(producerFactory)

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
    ) = ShareKafkaListenerContainerFactory(shareConsumerFactory)
}
