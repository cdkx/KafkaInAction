package ru.eremin.common.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import ru.eremin.common.dto.OrderEvent;
import ru.eremin.common.exception.RetryableEventException;

import java.util.concurrent.TimeUnit;

public final class KafkaPublisher {

    private KafkaPublisher() {
    }

    public static void sendAndWait(KafkaTemplate<String, OrderEvent> kafkaTemplate,
                                   String topic,
                                   OrderEvent event) {
        try {
            kafkaTemplate.send(topic, event.getOrderId(), event).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableEventException("Kafka send interrupted", e);
        } catch (Exception e) {
            throw new RetryableEventException("Kafka send failed", e);
        }
    }
}
