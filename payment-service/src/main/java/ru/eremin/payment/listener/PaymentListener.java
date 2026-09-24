package ru.eremin.payment.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.eremin.common.config.KafkaProperties;
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
    private final KafkaProperties kafkaProperties;


    @KafkaListener(
            topics = "${app.kafka.topics.new-orders:new_orders}",
            groupId = "payment-service",
            concurrency = "${app.kafka.listener.concurrency:3}"
    )
    public void listen(OrderEvent event, Acknowledgment ack) {
        if (event == null || event.getOrderId() == null) {
            log.warn("[payment-service] Received empty event");
            ack.acknowledge();
            return;
        }

        log.info("[payment-service] Received order: orderId={}, status={}, eventType={}",
                event.getOrderId(), event.getStatus(), event.getEventType());

        if (event.getStatus() != OrderStatus.NEW) {
            log.info("[payment-service] Skipping non-new order: orderId={}, status={}",
                    event.getOrderId(), event.getStatus());
            ack.acknowledge();
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
            KafkaPublisher.sendAndWait(
                    kafkaTemplate,
                    kafkaProperties.getTopics().getPayedOrders(),
                    event);
        } catch (Exception e) {
            log.error("[payment-service] Failed to publish paid order: orderId={}. Will be retried by error handler",
                    event.getOrderId(), e);
            throw e;
        }

        log.info("[payment-service] Published paid order: orderId={}, topic={}",
                event.getOrderId(), kafkaProperties.getTopics().getPayedOrders());

        ack.acknowledge();
    }
}
