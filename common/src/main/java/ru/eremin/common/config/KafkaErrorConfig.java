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
import org.springframework.lang.NonNull;
import org.springframework.util.backoff.ExponentialBackOff;
import ru.eremin.common.exception.NonRetryableEventException;


@Slf4j
@Configuration
public class KafkaErrorConfig {
    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<?, ?> kafkaTemplate) {
        DefaultErrorHandler errorHandler = createErrorHandler(kafkaTemplate);
        errorHandler.addNotRetryableExceptions(
                DeserializationException.class,
                NonRetryableEventException.class
        );
        errorHandler.setAckAfterHandle(true);
        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(@NonNull ConsumerRecord<?, ?> consumerRecord,
                                       @NonNull Exception ex,
                                       int attempt) {
                log.error("[kafka-error-handler] failed delivery: topic={} key={} attempt={}. Reason: {}",
                        consumerRecord.topic(), consumerRecord.key(), attempt, ex.getMessage());
            }

            @Override
            public void recovered(@NonNull ConsumerRecord<?, ?> consumerRecord,
                                  @NonNull Exception ex) {
                log.warn("[kafka-error-handler] sent to DLT: topic={} key={}",
                        consumerRecord.topic(), consumerRecord.key());
            }
        });
        return errorHandler;
    }

    private DefaultErrorHandler createErrorHandler(KafkaTemplate<?, ?> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (consumerRecord, ex)
                        -> new TopicPartition(consumerRecord.topic() + ".DLT", consumerRecord.partition())
        ) {
            @Override
            public void accept(@NonNull ConsumerRecord<?, ?> consumerRecord,
                               @NonNull Exception ex) {
                log.debug("[kafka-error-handler] publishing to DLT: topic={} partition={} offset={} key={}",
                        consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key());
                super.accept(consumerRecord, ex);
            }
        };

        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(500L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5000L);
        backOff.setMaxElapsedTime(15000L);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}
