package com.github.eyefloaters.health;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import jakarta.inject.Inject;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.jboss.logging.Logger;

abstract class CounterProgressCheck {

    @Inject
    Logger log;

    private final String checkName;

    Map<String, Map<TopicPartition, Long>> prevRecordCounts = Collections.emptyMap();
    Map<String, Map<TopicPartition, Instant>> latestRecordActivityTimes = new ConcurrentHashMap<>();

    protected CounterProgressCheck(String checkName) {
        this.checkName = checkName;
    }

    HealthCheckResponse check(Map<String, Map<TopicPartition, Long>> recordsCounts) {
        var builder = HealthCheckResponse.builder()
                .name(checkName);

        if (!prevRecordCounts.isEmpty()) {
            Instant now = Instant.now();

            prevRecordCounts.forEach((clusterKey, prevCounts) ->
                prevCounts.forEach((partition, prevCount) -> {
                    long currentCount = recordsCounts.get(clusterKey).get(partition);

                    if (currentCount > prevCount) {
                        log.debugf("Counter %s/%s increased from %d to %d", checkName, partition, prevCount, currentCount);
                        latestRecordActivityTimes
                            .computeIfAbsent(clusterKey, k -> new ConcurrentHashMap<>(prevCounts.size()))
                            .put(partition, now);
                    } else {
                        log.infof("Counter %s/%s unchanged from %d", checkName, partition, prevCount);
                    }
                }));
        }

        Instant inactiveTime = Instant.now().minus(Duration.ofMinutes(5));

        long inactivePartitions = latestRecordActivityTimes.values()
                .stream()
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .map(Map.Entry::getValue)
                .filter(lastTimeCounted -> lastTimeCounted.isBefore(inactiveTime))
                .count();
        String earliestActivity = latestRecordActivityTimes.values()
                .stream()
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .map(Map.Entry::getValue)
                .min(Instant::compareTo)
                .map(Object::toString)
                .orElse("NA");
        String latestActivity = latestRecordActivityTimes.values()
                .stream()
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .map(Map.Entry::getValue)
                .max(Instant::compareTo)
                .map(Object::toString)
                .orElse("NA");
        long currentCount = recordsCounts
                .values()
                .stream()
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .mapToLong(Map.Entry::getValue)
                .sum();

        builder.withData("currentCount", currentCount);
        builder.withData("inactivePartitions", inactivePartitions);
        builder.withData("earliestActivity", earliestActivity);
        builder.withData("latestActivity", latestActivity);

        boolean up = true;

        if (inactivePartitions > 0) {
            up = false;
        }

        prevRecordCounts = recordsCounts
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> Map.copyOf(e.getValue())));

        return builder.status(up).build();
    }
}
