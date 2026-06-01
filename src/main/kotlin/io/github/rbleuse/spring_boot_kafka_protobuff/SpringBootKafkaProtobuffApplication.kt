package io.github.rbleuse.spring_boot_kafka_protobuff

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class SpringBootKafkaProtobuffApplication

fun main(args: Array<String>) {
	runApplication<SpringBootKafkaProtobuffApplication>(*args)
}
