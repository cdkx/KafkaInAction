package ru.eremin.notifications.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.eremin.common.dto.OrderEvent;
import ru.eremin.common.dto.OrderStatus;

import static ru.eremin.common.kafka.EventHeaders.CORRELATION_ID;


@Slf4j
@Component
public class NotificationListener {

    @KafkaListener(
            topics = "${app.kafka.topics.sent-orders:sent_orders}",
            groupId = "notifications-service",
            concurrency = "${app.kafka.listener.concurrency:3}"
    )
    public void listen(OrderEvent event,
                       @Header(value = CORRELATION_ID, required = false) String correlationId,
                       Acknowledgment ack
    ) {
        String cid = correlationId != null ? correlationId : "unknown";

        if (event == null || event.getOrderId() == null) {
            log.warn("[notifications-service] correlationId={} Received empty event", cid);
            ack.acknowledge();
            return;
        }

        log.info("[notifications-service] correlationId={} Received shipped order: orderId={}, userId={}, status={}",
                cid, event.getOrderId(), event.getUserId(), event.getStatus());

        if (event.getStatus() != OrderStatus.SENT) {
            log.info("[notifications-service] correlationId={} Skipping non-sent order: orderId={}, status={}",
                    cid, event.getOrderId(), event.getStatus());
            ack.acknowledge();
            return;
        }

        log.info("[notifications-service] correlationId={} Notification sent to user: userId={}, orderId={}, message={}",
                cid,
                event.getUserId(),
                event.getOrderId(),
                "Your order has been successfully delivered");

        ack.acknowledge();
    }
}
