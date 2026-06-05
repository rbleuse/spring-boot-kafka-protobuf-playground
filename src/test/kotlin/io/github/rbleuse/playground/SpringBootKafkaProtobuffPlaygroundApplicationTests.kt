package io.github.rbleuse.playground

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest(properties = ["playground.kafka.listener-auto-startup=false"])
class SpringBootKafkaProtobuffPlaygroundApplicationTests {
    @Test
    fun contextLoads() {
    }
}
