package ru.eremin.notifications.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.eremin.common.dto.OrderEvent;
import ru.eremin.common.dto.OrderStatus;


@Slf4j
@Component
public class NotificationListener {

    @KafkaListener(
            topics = "${app.kafka.topics.sent-orders:sent_orders}",
            groupId = "notifications-service",
            concurrency = "${app.kafka.listener.concurrency:3}"
    )
    public void listen(OrderEvent event, Acknowledgment acknowledgment) {
        if (event == null || event.getOrderId() == null) {
            log.warn("[notifications-service] Received empty event");
            acknowledgment.acknowledge();
            return;
        }

        log.info("[notifications-service] Received shipped order: orderId={}, userId={}, status={}",
                event.getOrderId(), event.getUserId(), event.getStatus());

        if (event.getStatus() != OrderStatus.SENT) {
            log.info("[notifications-service] Skipping non-sent order: orderId={}", event.getOrderId());
            acknowledgment.acknowledge();
            return;
        }

        log.info("[notifications-service] Notification sent to user: userId={}, orderId={}, message={}",
                event.getUserId(),
                event.getOrderId(),
                "Your order has been successfully delivered");

        acknowledgment.acknowledge();
    }
}
