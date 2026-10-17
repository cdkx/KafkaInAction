package ru.eremin.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import ru.eremin.common.config.KafkaErrorConfig;
import ru.eremin.common.config.KafkaTopicConfig;

@SpringBootApplication
@Import({KafkaTopicConfig.class, KafkaErrorConfig.class})
public class OrdersServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrdersServiceApplication.class, args);
    }
}
