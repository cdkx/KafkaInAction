package ru.eremin.payment.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.eremin.common.dto.OrderEvent;
import ru.eremin.common.dto.OrderStatus;
import ru.eremin.common.exception.NonRetryableEventException;
import ru.eremin.common.kafka.KafkaPublisher;

import java.math.BigDecimal;
import java.time.Instant;


@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentListener {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Value("${app.kafka.topics.payed-orders:payed_orders}")
    private String payedOrdersTopic;

    @KafkaListener(
            topics = "${app.kafka.topics.new-orders:new_orders}",
            groupId = "payment-service",
            concurrency = "${app.kafka.listener.concurrency:3}"
    )
    public void listen(OrderEvent event, Acknowledgment acknowledgment) {
        if (event == null || event.getOrderId() == null) {
            log.warn("[payment-service] Received empty event");
            acknowledgment.acknowledge();
            return;
        }

        log.info("[payment-service] Received order: orderId={}, status={}, eventType={}",
                event.getOrderId(), event.getStatus(), event.getEventType());

        if (event.getStatus() != OrderStatus.NEW) {
            log.info("[payment-service] Skipping non-new order: orderId={}, status={}",
                    event.getOrderId(), event.getStatus());
            acknowledgment.acknowledge();
            return;
        }

        if (event.getAmount() == null || event.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            log.error("[payment-service] Payment validation FAILED: orderId={}, amount={}. " +
                            "Message will be routed to DLQ without retries",
                    event.getOrderId(), event.getAmount());
            throw new NonRetryableEventException("Invalid amount for order " + event.getOrderId());
        }

        event.setStatus(OrderStatus.PAID);
        event.setEventType("PAYMENT_COMPLETED");
        event.setUpdatedAt(Instant.now());
        event.setDetails("Payment approved");

        log.info("[payment-service] Payment completed: orderId={}", event.getOrderId());

        try {
            KafkaPublisher.sendAndWait(kafkaTemplate, payedOrdersTopic, event);
        } catch (Exception e) {
            log.error("[payment-service] Failed to publish paid order: orderId={}. Will be retried by error handler",
                    event.getOrderId(), e);
            throw e;
        }

        log.info("[payment-service] Published paid order: orderId={}, topic={}", event.getOrderId(), payedOrdersTopic);

        acknowledgment.acknowledge();
    }
}
