package com.github.streamshub.health;

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
import org.jboss.logging.Logger.Level;

abstract class CounterProgressCheck {

    static final Instant STARTUP = Instant.now();
    static final Duration MINS3 = Duration.ofMinutes(3);
    static final Duration MINS4 = Duration.ofMinutes(4);
    static final Duration MINS5 = Duration.ofMinutes(5);

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
                            .computeIfAbsent(clusterKey, _ -> new ConcurrentHashMap<>(prevCounts.size()))
                            .put(partition, now);
                    } else {
                        var lastUpdate = latestRecordActivityTimes
                            .computeIfAbsent(clusterKey, _ -> new ConcurrentHashMap<>(prevCounts.size()))
                            .getOrDefault(partition, STARTUP);

                        Duration timeStale = Duration.between(lastUpdate, now);
                        Level level;

                        if (timeStale.compareTo(MINS3) < 0) {
                            level = Level.DEBUG;
                        } else if (timeStale.compareTo(MINS4) < 0) {
                            level = Level.INFO;
                        } else {
                            level = Level.WARN;
                        }

                        log.logf(level, "Counter %s/%s in cluster %s unchanged from %d at %s",
                                checkName, partition, clusterKey, prevCount, lastUpdate);

                        latestRecordActivityTimes
                            .computeIfAbsent(clusterKey, _ -> new ConcurrentHashMap<>(prevCounts.size()))
                            // Only set the time if not set since startup
                            .putIfAbsent(partition, now);
                    }
                }));
        }

        Instant inactiveTime = Instant.now().minus(MINS5);

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
                .orElse("none");
        String latestActivity = latestRecordActivityTimes.values()
                .stream()
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .map(Map.Entry::getValue)
                .max(Instant::compareTo)
                .map(Object::toString)
                .orElse("none");
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
