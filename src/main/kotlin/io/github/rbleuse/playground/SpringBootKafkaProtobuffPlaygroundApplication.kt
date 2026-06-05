package io.github.rbleuse.playground

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class SpringBootKafkaProtobuffPlaygroundApplication

fun main(args: Array<String>) {
    runApplication<SpringBootKafkaProtobuffPlaygroundApplication>(*args)
}
