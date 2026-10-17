package ru.eremin.payment.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.eremin.common.config.KafkaProperties;
import ru.eremin.common.dto.OrderEvent;
import ru.eremin.common.dto.OrderStatus;
import ru.eremin.common.exception.NonRetryableEventException;
import ru.eremin.common.kafka.KafkaPublisher;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static ru.eremin.common.kafka.EventHeaders.CORRELATION_ID;


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
    public void listen(OrderEvent event,
                       @Header(value = CORRELATION_ID, required = false) String correlationId,
                       Acknowledgment ack
    ) {
        String cid = correlationId != null ? correlationId : UUID.randomUUID().toString();

        if (event == null || event.getOrderId() == null) {
            log.warn("[payment-service] correlationId={} Received empty event", cid);
            ack.acknowledge();
            return;
        }

        log.info("[payment-service] correlationId={} Received order: orderId={}, status={}, eventType={}",
                cid, event.getOrderId(), event.getStatus(), event.getEventType());

        if (event.getStatus() != OrderStatus.NEW) {
            log.info("[payment-service] correlationId={} Skipping non-new order: orderId={}, status={}",
                    cid, event.getOrderId(), event.getStatus());
            ack.acknowledge();
            return;
        }

        if (event.getAmount() == null || event.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            log.error("[payment-service] correlationId={} Payment validation FAILED: orderId={}, amount={}. "
                            + "Message will be routed to DLQ without retries",
                    cid, event.getOrderId(), event.getAmount());
            throw new NonRetryableEventException("Invalid amount for order " + event.getOrderId());
        }

        event.setStatus(OrderStatus.PAID);
        event.setEventType("PAYMENT_COMPLETED");
        event.setUpdatedAt(Instant.now());
        event.setDetails("Payment approved");

        log.info("[payment-service] correlationId={} Payment completed: orderId={}", cid, event.getOrderId());

        try {
            KafkaPublisher.sendAndWait(
                    kafkaTemplate,
                    kafkaProperties.getTopics().getPayedOrders(),
                    event,
                    cid
            );
        } catch (Exception e) {
            log.error("[payment-service] correlationId={} Failed to publish paid order: orderId={}. "
                            + "Will be retried by error handler",
                    cid, event.getOrderId(), e);
            throw e;
        }

        log.info("[payment-service] correlationId={} Published paid order: orderId={}, topic={}",
                cid, event.getOrderId(), kafkaProperties.getTopics().getPayedOrders());

        ack.acknowledge();
    }
}
