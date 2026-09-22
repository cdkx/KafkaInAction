package ru.eremin.common.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.partitions:6}")
    private int partitions;

    @Value("${app.kafka.replication:1}")
    private int replication;

    @Value("${app.kafka.topics.new-orders:new_orders}")
    private String newOrdersTopic;

    @Value("${app.kafka.topics.payed-orders:payed_orders}")
    private String payedOrdersTopic;

    @Value("${app.kafka.topics.sent-orders:sent_orders}")
    private String sentOrdersTopic;

    @Bean
    public NewTopic newOrders() {
        return TopicBuilder.name(newOrdersTopic)
                .partitions(partitions)
                .replicas(replication)
                .build();
    }

    @Bean
    public NewTopic payedOrders() {
        return TopicBuilder.name(payedOrdersTopic)
                .partitions(partitions)
                .replicas(replication)
                .build();
    }

    @Bean
    public NewTopic sentOrders() {
        return TopicBuilder.name(sentOrdersTopic)
                .partitions(partitions)
                .replicas(replication)
                .build();
    }

    @Bean
    public NewTopic newOrdersDlt() {
        return TopicBuilder.name(newOrdersTopic + ".DLT")
                .partitions(partitions)
                .replicas(replication)
                .build();
    }

    @Bean
    public NewTopic payedOrdersDlt() {
        return TopicBuilder.name(payedOrdersTopic + ".DLT")
                .partitions(partitions)
                .replicas(replication)
                .build();
    }

    @Bean
    public NewTopic sentOrdersDlt() {
        return TopicBuilder.name(sentOrdersTopic + ".DLT")
                .partitions(partitions)
                .replicas(replication)
                .build();
    }
}
