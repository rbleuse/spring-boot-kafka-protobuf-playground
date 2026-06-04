package io.github.rbleuse.playground.config

import jakarta.annotation.PostConstruct
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.common.config.ConfigResource
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.stereotype.Component

/**
 * Applies the share group's dynamic config before the listener container starts consuming.
 *
 * Runs in `@PostConstruct`, which completes for every singleton before any `SmartLifecycle`
 * (including the Kafka listener container) is started, so the share group is configured to
 * read from the earliest offset before the first poll. Skipped when the listener is not
 * auto-started, since that mode is used by context-only tests with no broker available.
 */
@Component
class ShareGroupConfigurer(
    private val kafkaProperties: KafkaProperties,
    @Value("\${playground.kafka.share-group}") private val shareGroup: String,
    @Value("\${playground.kafka.listener-auto-startup:true}") private val listenerAutoStartup: Boolean,
) {
    @PostConstruct
    fun configureShareGroup() {
        if (!listenerAutoStartup) {
            return
        }
        AdminClient.create(kafkaProperties.buildAdminProperties()).use { admin ->
            admin
                .incrementalAlterConfigs(
                    mapOf(
                        ConfigResource(ConfigResource.Type.GROUP, shareGroup) to
                            listOf(
                                AlterConfigOp(
                                    ConfigEntry("share.auto.offset.reset", "earliest"),
                                    AlterConfigOp.OpType.SET,
                                ),
                            ),
                    ),
                ).all()
                .get()
        }
    }
}
