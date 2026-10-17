package ru.eremin.shipping.listener;

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
import ru.eremin.common.kafka.KafkaPublisher;

import java.time.Instant;
import java.util.UUID;

import static ru.eremin.common.kafka.EventHeaders.CORRELATION_ID;


@Slf4j
@Component
@RequiredArgsConstructor
public class ShippingListener {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final KafkaProperties kafkaProperties;

    @KafkaListener(
            topics = "${app.kafka.topics.payed-orders:payed_orders}",
            groupId = "shipping-service",
            concurrency = "${app.kafka.listener.concurrency:3}"
    )
    public void listen(OrderEvent event,
                       @Header(value = CORRELATION_ID, required = false) String correlationId,
                       Acknowledgment ack
    ) {
        String cid = correlationId != null ? correlationId : UUID.randomUUID().toString();

        if (event == null || event.getOrderId() == null) {
            log.warn("[shipping-service] correlationId={} Received empty event", cid);
            ack.acknowledge();
            return;
        }

        log.info("[shipping-service] correlationId={} Received paid order: orderId={}, status={}, eventType={}",
                cid, event.getOrderId(), event.getStatus(), event.getEventType());

        if (event.getStatus() != OrderStatus.PAID) {
            log.info("[shipping-service] correlationId={} Skipping non-paid order: orderId={}, status={}",
                    cid, event.getOrderId(), event.getStatus());
            ack.acknowledge();
            return;
        }

        event.setStatus(OrderStatus.SENT);
        event.setEventType("SHIPMENT_COMPLETED");
        event.setUpdatedAt(Instant.now());
        event.setDetails("Order packed and shipped");

        log.info("[shipping-service] correlationId={} Order shipped: orderId={}", cid, event.getOrderId());

        try {
            KafkaPublisher.sendAndWait(
                    kafkaTemplate,
                    kafkaProperties.getTopics().getSentOrders(),
                    event,
                    cid
            );
        } catch (Exception e) {
            log.error("[shipping-service] correlationId={} Failed to publish paid order: orderId={}. "
                            + "Will be retried by error handler",
                    cid, event.getOrderId(), e);
            throw e;
        }

        log.info("[shipping-service] correlationId={} Published shipped order: orderId={}, topic={}",
                cid, event.getOrderId(), kafkaProperties.getTopics().getPayedOrders());

        ack.acknowledge();
    }
}
