package ru.eremin.shipping.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.eremin.common.dto.OrderEvent;
import ru.eremin.common.dto.OrderStatus;
import ru.eremin.common.kafka.KafkaPublisher;

import java.time.Instant;


@Slf4j
@Component
@RequiredArgsConstructor
public class ShippingListener {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Value("${app.kafka.topics.sent-orders:sent_orders}")
    private String sentOrdersTopic;

    @KafkaListener(
            topics = "${app.kafka.topics.payed-orders:payed_orders}",
            groupId = "shipping-service",
            concurrency = "${app.kafka.listener.concurrency:3}"
    )
    public void listen(OrderEvent event, Acknowledgment acknowledgment) {
        if (event == null || event.getOrderId() == null) {
            log.warn("[shipping-service] Received empty event");
            acknowledgment.acknowledge();
            return;
        }

        log.info("[shipping-service] Received paid order: orderId={}, status={}",
                event.getOrderId(), event.getStatus());

        if (event.getStatus() != OrderStatus.PAID) {
            log.info("[shipping-service] Skipping non-paid order: orderId={}, status={}",
                    event.getOrderId(), event.getStatus());
            acknowledgment.acknowledge();
            return;
        }

        event.setStatus(OrderStatus.SENT);
        event.setEventType("SHIPMENT_COMPLETED");
        event.setUpdatedAt(Instant.now());
        event.setDetails("Order packed and shipped");

        log.info("[shipping-service] Order shipped: orderId={}", event.getOrderId());

        try {
            KafkaPublisher.sendAndWait(kafkaTemplate, sentOrdersTopic, event);
        } catch (Exception e) {
            log.error("[shipping-service] Failed to publish order: orderId={}. Will be retried by error handler",
                    event.getOrderId(), e);
            throw e;
        }

        log.info("[shipping-service] Published shipped order: orderId={}, topic={}",
                event.getOrderId(), sentOrdersTopic);

        acknowledgment.acknowledge();
    }
}
