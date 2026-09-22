package ru.eremin.common.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.ExponentialBackOff;
import ru.eremin.common.exception.NonRetryableEventException;


@Slf4j
@Configuration
public class KafkaErrorConfig {
    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<?, ?> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition())
        ) {
            @Override
            public void accept(ConsumerRecord<?, ?> record, Exception ex) {
                log.error("[kafka-error-handler] Record topic={} partition={} offset={} key={} -> sending to DLT {}. Reason: {}",
                        record.topic(), record.partition(), record.offset(), record.key(),
                        record.topic() + ".DLT", ex.getMessage());
                super.accept(record, ex);
            }
        };

        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(500L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5000L);
        backOff.setMaxElapsedTime(15000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(
                DeserializationException.class,
                NonRetryableEventException.class
        );
        errorHandler.setAckAfterHandle(true);

        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.error("[kafka-error-handler] Failed to process record: topic={} key={} attempt={}. Reason: {}",
                        record.topic(), record.key(), deliveryAttempt, ex.getMessage());
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.warn("[kafka-error-handler] Record topic={} key={} recovered (DLT), processing continues",
                        record.topic(), record.key());
            }
        });

        return errorHandler;
    }
}
