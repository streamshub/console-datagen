package com.github.eyefloaters;

import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.inject.Inject;
import jakarta.json.Json;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

@ApplicationScoped
public class DataGenerator {

    @Inject
    Logger log;

    @Inject
    ManagedExecutor exec;

    @Inject
    @Channel("timestamps-out")
    Emitter<String> timestampEmitter;

    @Inject
    Admin adminClient;

    AtomicBoolean running = new AtomicBoolean(true);

    long recordsProduced = 0;
    long recordsConsumed = 0;

    void start(@Observes Startup startupEvent /* NOSONAR */) {
        Random generator = new Random();
        byte[] buffer = new byte[1000];

        exec.submit(() -> {
            while (running.get()) {
                long start = System.currentTimeMillis();
                long rate = 100 * ((start / 10000) % 5) + 10;

                for (int i = 0; i < rate; i++) {
                    generator.nextBytes(buffer);
                    String value = Json.createObjectBuilder()
                            .add("timestamp", Instant.now().toString())
                            .add("payload", Base64.getEncoder().encodeToString(buffer))
                            .build()
                            .toString();

                    timestampEmitter.send(value);

                    if (++recordsProduced % 10_000 == 0) {
                        log.infof("Produced %d records (since startup)", recordsProduced);
                    }
                }

                long end = System.currentTimeMillis();
                long sleepTime = Math.max(0, 1000 - (end - start));

                log.debugf("Produced %d messages in %dms, sleeping %dms", rate, end - start, sleepTime);

                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    log.warn("Producer thread was interrupted, breaking from loop");
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    void stop(@Observes Shutdown shutdownEvent /* NOSONAR */) {
        running.set(false);
    }

    @Incoming("timestamps")
    public void consume(ConsumerRecord<String, String> rec) {
        if (++recordsConsumed % 10_000 == 0) {
            log.infof("Consumed %d records, latest is partition %d, offset %d",
                    recordsConsumed, rec.partition(), rec.offset());

            var partition = new TopicPartition(rec.topic(), rec.partition());
            var earliest = getOffset(partition, OffsetSpec.earliest());
            var latest = getOffset(partition, OffsetSpec.latest());

            CompletableFuture.allOf(earliest, latest)
                .thenCompose(nothing -> {
                    long diff = latest.join() - earliest.join();

                    if (diff >= 1_000_000) {
                        log.infof("Offset diff is %d, truncating topic %s, partition %d to offset %d",
                                diff, rec.topic(), rec.partition(), rec.offset());
                        // Truncate the topic to the up to the previous offset
                        return adminClient.deleteRecords(Map.of(new TopicPartition(rec.topic(), rec.partition()),
                                RecordsToDelete.beforeOffset(rec.offset())))
                            .all()
                            .toCompletionStage();
                    } else {
                        log.infof("Offset diff is %d for topic %s, partition %d at offset %d",
                                diff, rec.topic(), rec.partition(), rec.offset());
                        return CompletableFuture.completedStage(null);
                    }
                })
                .join();
        }
    }

    CompletableFuture<Long> getOffset(TopicPartition partition, OffsetSpec spec) {
        return adminClient.listOffsets(Map.of(partition, spec))
            .partitionResult(partition)
            .toCompletionStage()
            .thenApply(ListOffsetsResultInfo::offset)
            .toCompletableFuture();
    }
}
