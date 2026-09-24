package ru.eremin.common.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;


@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaTopicConfig {
    private final KafkaProperties kafkaProperties;

    @Bean
    public NewTopic newOrders() {
        return TopicBuilder.name(kafkaProperties.getTopics().getNewOrders())
                .partitions(kafkaProperties.getPartitions())
                .replicas(kafkaProperties.getReplication())
                .build();
    }

    @Bean
    public NewTopic payedOrders() {
        return TopicBuilder.name(kafkaProperties.getTopics().getPayedOrders())
                .partitions(kafkaProperties.getPartitions())
                .replicas(kafkaProperties.getReplication())
                .build();
    }

    @Bean
    public NewTopic sentOrders() {
        return TopicBuilder.name(kafkaProperties.getTopics().getSentOrders())
                .partitions(kafkaProperties.getPartitions())
                .replicas(kafkaProperties.getReplication())
                .build();
    }

    @Bean
    public NewTopic newOrdersDlt() {
        return TopicBuilder.name(kafkaProperties.getTopics().getNewOrders() + ".DLT")
                .partitions(kafkaProperties.getPartitions())
                .replicas(kafkaProperties.getReplication())
                .build();
    }

    @Bean
    public NewTopic payedOrdersDlt() {
        return TopicBuilder.name(kafkaProperties.getTopics().getPayedOrders() + ".DLT")
                .partitions(kafkaProperties.getPartitions())
                .replicas(kafkaProperties.getReplication())
                .build();
    }

    @Bean
    public NewTopic sentOrdersDlt() {
        return TopicBuilder.name(kafkaProperties.getTopics().getSentOrders() + ".DLT")
                .partitions(kafkaProperties.getPartitions())
                .replicas(kafkaProperties.getReplication())
                .build();
    }
}
