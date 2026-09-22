package ru.eremin.notifications;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import ru.eremin.common.config.KafkaErrorConfig;
import ru.eremin.common.config.KafkaTopicConfig;


@EnableKafka
@SpringBootApplication
@Import({KafkaTopicConfig.class, KafkaErrorConfig.class})
public class NotificationsServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationsServiceApplication.class, args);
    }
}
