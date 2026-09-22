package ru.eremin.common.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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

        if (numPartitions <= 1) {
            return 0;
        }

        if (keyBytes == null || keyBytes.length == 0) {
            int next = counter.getAndIncrement();
            return (next & Integer.MAX_VALUE) % numPartitions;
        }

        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public void close() {

    }
}
