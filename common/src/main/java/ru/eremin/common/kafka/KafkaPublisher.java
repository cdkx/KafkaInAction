package ru.eremin.common.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import ru.eremin.common.dto.OrderEvent;
import ru.eremin.common.exception.RetryableEventException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static ru.eremin.common.kafka.EventHeaders.CORRELATION_ID;

public final class KafkaPublisher {

    private KafkaPublisher() {
    }

    public static void sendAndWait(KafkaTemplate<String, OrderEvent> kafkaTemplate,
                                   String topic,
                                   OrderEvent event,
                                   String correlationId
    ) {
        ProducerRecord<String, OrderEvent> producerRecord = new ProducerRecord<>(
                topic, null, event.getOrderId(), event);

        if (correlationId != null && !correlationId.isBlank()) {
            producerRecord.headers().add(
                    CORRELATION_ID,
                    correlationId.getBytes(StandardCharsets.UTF_8));
        }

        try {
            kafkaTemplate.send(producerRecord).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableEventException("Kafka send interrupted", e);
        } catch (Exception e) {
            throw new RetryableEventException("Kafka send failed", e);
        }
    }
}
