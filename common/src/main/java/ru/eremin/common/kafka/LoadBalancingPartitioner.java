package ru.eremin.common.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public class LoadBalancingPartitioner implements Partitioner {
    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public int partition(String topic,
                         Object key,
                         byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster) {

        int numPartitions = cluster.partitionCountForTopic(topic);
        int partition;

        if (numPartitions <= 1) {
            partition = 0;
        } else if (keyBytes == null || keyBytes.length == 0) {
            int next = counter.getAndIncrement();
            partition = (next & Integer.MAX_VALUE) % numPartitions;
        } else {
            partition = Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }

        log.debug("[LoadBalancingPartitioner] topic={} key={} -> partition={} of {}",
                topic, key, partition, numPartitions);

        return partition;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() {
    }
}
