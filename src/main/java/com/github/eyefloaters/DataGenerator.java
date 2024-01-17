package com.github.eyefloaters;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.json.Json;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

@ApplicationScoped
public class DataGenerator {

    static final String TOPIC_NAME_TEMPLATE = "console_datagen_%03d-%s";
    static final String CLUSTER_NAME_KEY = "cluster.name";

    @Inject
    Logger log;

    @Inject
    @ConfigProperty(name = "datagen.enabled", defaultValue = "true")
    boolean datagenEnabled;

    @Inject
    @ConfigProperty(name = "datagen.consumer-groups", defaultValue = "1")
    int consumerCount;

    @Inject
    @ConfigProperty(name = "datagen.topics-per-consumer", defaultValue = "1")
    int topicsPerConsumer;

    @Inject
    @ConfigProperty(name = "datagen.partitions-per-topic", defaultValue = "1")
    int partitionsPerTopic;

    @Inject
    @ConfigProperty(name = "datagen.topic-name-template", defaultValue = TOPIC_NAME_TEMPLATE)
    String topicNameTemplate;

    @Inject
    @Named("adminConfigs")
    Map<String, Map<String, Object>> adminConfigs;

    @Inject
    @Named("producerConfigs")
    Map<String, Map<String, Object>> producerConfigs;

    @Inject
    @Named("consumerConfigs")
    Map<String, Map<String, Object>> consumerConfigs;

    static ExecutorService virtualExec = Executors.newVirtualThreadPerTaskExecutor();

    AtomicBoolean running = new AtomicBoolean(true);
    Random generator = new Random();

    @Produces
    @ApplicationScoped
    @Named("adminClients")
    Map<String, Admin> adminClients = new HashMap<>();

    @Produces
    @ApplicationScoped
    @Named("recordsProduced")
    Map<String, Map<TopicPartition , Long>> recordsProduced = new ConcurrentHashMap<>();

    @Produces
    @ApplicationScoped
    @Named("recordsConsumed")
    Map<String, Map<TopicPartition , Long>> recordsConsumed = new ConcurrentHashMap<>();

    void start(@Observes Startup startupEvent /* NOSONAR */) {
        if (!datagenEnabled) {
            log.info("datagen.enabled=false ; producers and consumers will not be started");
            return;
        }

        AtomicInteger clientCount = new AtomicInteger(0);

        adminConfigs.forEach((clusterKey, configProperties) ->
            virtualExec.submit(() -> {
                MDC.put(CLUSTER_NAME_KEY, clusterKey);

                Admin adminClient = Admin.create(configProperties);
                adminClients.put(clusterKey, adminClient);

                var allTopics = IntStream
                        .range(0, consumerCount)
                        .mapToObj(groupNumber -> IntStream
                                .range(0, topicsPerConsumer)
                                .mapToObj(t -> topicNameTemplate.formatted(groupNumber, (char) ('a' + t))))
                        .flatMap(Function.identity())
                        .toList();

                initialize(clusterKey, adminClient, allTopics, partitionsPerTopic);

                IntStream.range(0, consumerCount).forEach(groupNumber -> {
                    var topics = IntStream.range(0, topicsPerConsumer)
                            .mapToObj(t -> topicNameTemplate.formatted(groupNumber, (char) ('a' + t)))
                            .toList();

                    virtualExec.submit(() -> {
                        var configs = new HashMap<>(producerConfigs.get(clusterKey));
                        String clientId = "console-datagen-producer-" + groupNumber + "-" + clientCount.incrementAndGet();
                        configs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

                        MDC.put(CLUSTER_NAME_KEY, clusterKey);
                        MDC.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

                        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfigs.get(clusterKey))) {
                            while (running.get()) {
                                produce(clusterKey, producer, topics);
                            }
                        } catch (Exception e) {
                            log.warnf(e, "Error producing: %s", e.getMessage());
                        }

                        log.infof("Run loop complete for producer %d on cluster %s", groupNumber, clusterKey);
                    });

                    virtualExec.submit(() -> {
                        var configs = new HashMap<>(consumerConfigs.get(clusterKey));
                        String groupId = "console-datagen-group-" + groupNumber;
                        String clientId = "console-datagen-consumer-" + groupNumber + "-" + clientCount.incrementAndGet();

                        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

                        MDC.put(CLUSTER_NAME_KEY, clusterKey);
                        MDC.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

                        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(configs)) {
                            consumer.subscribe(topics);

                            while (running.get()) {
                                consumer.poll(Duration.ofSeconds(2)).forEach(rec -> consume(clusterKey, rec));
                            }
                        } catch (Exception e) {
                            log.warnf(e, "Error consuming: %s", e.getMessage());
                        }

                        log.infof("Run loop complete for consumer group %s", groupId);
                    });
                });
            }));
    }

    void stop(@Observes Shutdown shutdownEvent /* NOSONAR */) throws Exception {
        running.set(false);

        adminClients.forEach((clusterKey, client) -> {
            log.infof("Stopping Admin client for cluster %s...", clusterKey);
            client.close();
            log.infof("Admin client for cluster %s closed", clusterKey);
        });

        virtualExec.shutdown();
        virtualExec.awaitTermination(10, TimeUnit.SECONDS);
    }

    void initialize(String clusterKey, Admin adminClient, List<String> topics, int partitionsPerTopic) {
        adminClient.describeCluster()
            .clusterId()
            .toCompletionStage()
            .thenAccept(clusterId -> {
                MDC.put(CLUSTER_NAME_KEY, clusterKey);
                log.infof("Initializing cluster %s (id=%s)", clusterKey, clusterId);
            })
            .toCompletableFuture()
            .join();

        List<String> dataGenGroups = adminClient.listConsumerGroups(new ListConsumerGroupsOptions()
                .inStates(Set.of(ConsumerGroupState.EMPTY)))
            .all()
            .toCompletionStage()
            .toCompletableFuture()
            .join()
            .stream()
            .map(ConsumerGroupListing::groupId)
            .filter(name -> name.startsWith("console-datagen-group-"))
            .collect(Collectors.toCollection(ArrayList::new));

        log.infof("Deleting existing consumer groups %s", dataGenGroups);

        adminClient.deleteConsumerGroups(dataGenGroups)
            .deletedGroups()
            .entrySet()
            .stream()
            .map(e -> e.getValue().toCompletionStage().exceptionally(error -> {
                if (error instanceof CompletionException ce) {
                    error = ce.getCause();
                }

                if (error instanceof GroupNotEmptyException) {
                    log.warnf("Consumer group %s is not empty and cannot be deleted", e.getKey());
                } else if (error instanceof GroupIdNotFoundException) {
                    // Ignore
                } else {
                    log.warnf(error, "Error deleting consumer group %s: %s", e.getKey(), error.getMessage());
                }
                return null;
            }))
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll())
            .join();

        log.infof("Deleting existing topics %s", topics);

        adminClient.deleteTopics(topics)
            .topicNameValues()
            .entrySet()
            .stream()
            .map(e -> e.getValue().toCompletionStage().exceptionally(error -> {
                if (error instanceof CompletionException ce) {
                    error = ce.getCause();
                }

                if (!(error instanceof UnknownTopicOrPartitionException)) {
                    log.warnf(error, "Error deleting topic %s: %s", e.getKey(), error.getMessage());
                }
                return null;
            }))
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll())
            .thenRun(() -> LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5)))
            .join();

        var newTopics = topics.stream()
                .map(t -> new NewTopic(t, partitionsPerTopic, (short) 3)
                        .configs(Map.of(
                                // 10 MiB
                                "segment.bytes", Integer.toString(10 * 1024 * 1024),
                                // 10 minutes
                                "segment.ms", Long.toString(TimeUnit.MINUTES.toMillis(10)))))
                .toList();

        log.infof("Creating topics: %s", topics);

        adminClient.createTopics(newTopics)
            .values()
            .entrySet()
            .stream()
            .map(e -> e.getValue().toCompletionStage().exceptionally(error -> {
                if (error instanceof CompletionException ce) {
                    error = ce.getCause();
                }

                log.warnf(error, "Error creating topic %s: %s", e.getKey(), error.getMessage());
                return null;
            }))
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll())
            .thenRun(() -> LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5)))
            .thenRun(() -> log.infof("Topics created: %s", topics))
            .join();

        Map<TopicPartition, Long> initialCounts = topics.stream()
                .flatMap(topic -> IntStream
                        .range(0, partitionsPerTopic)
                        .mapToObj(p -> new TopicPartition(topic, p)))
                .map(topicPartition -> Map.entry(topicPartition, 0L))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        recordsProduced.put(clusterKey, new ConcurrentHashMap<>(initialCounts));
        recordsConsumed.put(clusterKey, new ConcurrentHashMap<>(initialCounts));
    }

    void produce(String clusterKey, Producer<byte[], byte[]> producer, List<String> topics) {
        byte[] buffer = new byte[1000];
        int t = 0;
        long start = System.currentTimeMillis();
        long rate = 100 * ((start / 10000) % 5) + 10;


        for (int i = 0; i < rate; i++) {
            if (!running.get()) {
                return;
            }

            generator.nextBytes(buffer);

            byte[] value = Json.createObjectBuilder()
                    .add("timestamp", Instant.now().toString())
                    .add("payload", Base64.getEncoder().encodeToString(buffer))
                    .build()
                    .toString()
                    .getBytes();

            String topic = topics.get(t++ % topics.size());

            complete(producer.send(new ProducerRecord<>(topic, value)))
                .thenAccept(meta -> {
                    TopicPartition topicPartition = new TopicPartition(meta.topic(), meta.partition());
                    var currentCount = incrementAndGet(recordsProduced, clusterKey, topicPartition);

                    if (currentCount % 5_000 == 0) {
                        log.infof("Produced %d records to %s/%s (since startup)", currentCount, clusterKey, topicPartition);
                    }
                })
                .exceptionally(error -> {
                    log.warnf(error, "Error producing record: %s", error.getMessage());
                    return null;
                });
        }

        long end = System.currentTimeMillis();
        long sleepTime = Math.max(0, 1000 - (end - start));

        log.debugf("Produced %d messages in %dms, sleeping %dms", rate, end - start, sleepTime);
        if (running.get()) {
            LockSupport.parkUntil(System.currentTimeMillis() + sleepTime);
        }
    }

    public void consume(String clusterKey, ConsumerRecord<byte[], byte[]> rec) {
        TopicPartition topicPartition = new TopicPartition(rec.topic(), rec.partition());
        var currentCount = incrementAndGet(recordsConsumed, clusterKey, topicPartition);

        if (currentCount % 5_000 == 0) {
            log.infof("Consumed %d records from partition %s, latest is offset %d",
                    currentCount, topicPartition, rec.offset());
            maybeDeleteRecords(adminClients.get(clusterKey), topicPartition, rec.offset());
        }
    }

    long incrementAndGet(Map<String, Map<TopicPartition , Long>> counters, String clusterKey, TopicPartition topicPartition) {
        return counters.get(clusterKey)
            .compute(topicPartition, (k, v) -> v == null ? 1 : v + 1);
    }

    void maybeDeleteRecords(Admin adminClient, TopicPartition topicPartition, long offset) {
        var earliest = getOffset(adminClient, topicPartition, OffsetSpec.earliest());
        var latest = getOffset(adminClient, topicPartition, OffsetSpec.latest());

        CompletableFuture.allOf(earliest, latest)
            .thenComposeAsync(nothing -> {
                long diff = latest.join() - earliest.join();

                if (diff >= 5_000) {
                    log.infof("Offset diff is %d, truncating topic %s, partition %d to offset %d",
                            diff, topicPartition.topic(), topicPartition.partition(), offset);
                    // Truncate the topic to the up to the previous offset
                    var recordsToDelete = Map.of(topicPartition, RecordsToDelete.beforeOffset(offset));
                    return adminClient.deleteRecords(recordsToDelete)
                        .all()
                        .toCompletionStage();
                } else {
                    log.debugf("Offset diff is %d for topic %s, partition %d at offset %d",
                            diff, topicPartition.topic(), topicPartition.partition(), offset);
                    return CompletableFuture.completedStage(null);
                }
            }, virtualExec)
            .join();
    }

    CompletableFuture<Long> getOffset(Admin adminClient, TopicPartition partition, OffsetSpec spec) {
        return adminClient.listOffsets(Map.of(partition, spec))
            .partitionResult(partition)
            .toCompletionStage()
            .thenApply(ListOffsetsResultInfo::offset)
            .toCompletableFuture();
    }

    static <T> CompletableFuture<T> complete(Future<T> future) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            } catch (ExecutionException e) {
                throw new CompletionException(e.getCause());
            }
        }, virtualExec);
    }

    static <F extends Object> Collector<CompletableFuture<F>, ?, CompletableFuture<Void>> awaitingAll() {
        return Collectors.collectingAndThen(Collectors.toList(), pending ->
            CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)));
    }
}
