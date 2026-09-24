package ru.eremin.orders.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.eremin.common.config.KafkaProperties;
import ru.eremin.common.dto.OrderEvent;
import ru.eremin.common.dto.OrderStatus;
import ru.eremin.common.kafka.KafkaPublisher;
import ru.eremin.orders.dto.CreateOrderRequest;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final KafkaProperties kafkaProperties;


    private final Map<String, OrderEvent> orders = new ConcurrentHashMap<>();

    public OrderEvent createOrder(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        OrderEvent event = OrderEvent.builder()
                .orderId(orderId)
                .userId(request.getUserId())
                .amount(request.getAmount())
                .currency(request.getCurrency())
                .status(OrderStatus.NEW)
                .eventType("ORDER_CREATED")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .details(request.getComment())
                .build();

        orders.put(orderId, event);

        log.info("[orders-service] Creating order: orderId={}, userId={}", orderId, request.getUserId());

        KafkaPublisher.sendAndWait(kafkaTemplate, newOrdersTopic(), event);

        log.info("[orders-service] Order published to Kafka: orderId={}, topic={}", orderId, newOrdersTopic());

        return event;
    }

    public Optional<OrderEvent> updateStatus(String orderId, OrderStatus status) {
        OrderEvent updated = orders.computeIfPresent(orderId, (id, existing) -> {
            existing.setStatus(status);
            existing.setEventType("ORDER_STATUS_UPDATED");
            existing.setUpdatedAt(Instant.now());
            return existing;
        });

        if (updated != null) {
            log.info("[orders-service] Order status updated: orderId={}, newStatus={}", orderId, status);
            KafkaPublisher.sendAndWait(kafkaTemplate, newOrdersTopic(), updated);
        } else {
            log.warn("[orders-service] Order not found: orderId={}", orderId);
        }

        return Optional.ofNullable(updated);
    }

    public Optional<OrderEvent> getOrder(String orderId) {
        return Optional.ofNullable(orders.get(orderId));
    }

    private String newOrdersTopic() {
        return kafkaProperties.getTopics().getNewOrders();
    }
}
