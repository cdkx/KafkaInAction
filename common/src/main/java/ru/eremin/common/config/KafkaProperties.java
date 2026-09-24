package ru.eremin.common.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;


@Data
@Validated
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaProperties {

    @Min(value = 1, message = "Partitions must be at least 1")
    private int partitions = 6;

    @Min(value = 1, message = "Replication factor must be at least 1")
    private int replication = 1;

    @Valid
    private Topics topics = new Topics();

    @Data
    public static class Topics {
        @NotBlank(message = "Topic name cannot be blank")
        private String newOrders = "new_orders";

        @NotBlank(message = "Topic name cannot be blank")
        private String payedOrders = "payed_orders";

        @NotBlank(message = "Topic name cannot be blank")
        private String sentOrders = "sent_orders";
    }
}
