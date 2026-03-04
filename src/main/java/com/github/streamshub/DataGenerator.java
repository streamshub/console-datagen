package com.github.streamshub;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.json.JsonObject;
import jakarta.json.spi.JsonProvider;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import com.github.javafaker.Beer;
import com.github.javafaker.Faker;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

@ApplicationScoped
public class DataGenerator {

    static final String CLUSTER_NAME_KEY = "cluster.name";

    @Inject
    Logger log;

    @Inject
    DataGenConfig config;

    @Inject
    @ConfigProperty(name = "datagen.compression-types", defaultValue = "none")
    List<String> compressionTypes;

    @Inject
    @Named("adminConfigs")
    Map<String, Map<String, Object>> adminConfigs;

    @Inject
    @Named("producerConfigs")
    Map<String, Map<String, Object>> producerConfigs;

    @Inject
    @Named("consumerConfigs")
    Map<String, Map<String, Object>> consumerConfigs;

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

    static ExecutorService virtualExec = Executors.newVirtualThreadPerTaskExecutor();
    static JsonProvider json = JsonProvider.provider();

    AtomicBoolean running = new AtomicBoolean(true);
    Random generator;
    Faker faker;

    void start(@Observes Startup startupEvent /* NOSONAR */) {
        if (!config.enabled()) {
            log.info("datagen.enabled=false ; producers and consumers will not be started");
            return;
        }

        generator = new Random();
        faker = new Faker(generator);
        AtomicInteger clientCount = new AtomicInteger(0);

        adminConfigs.forEach((clusterKey, configProperties) ->
            virtualExec.submit(() -> {
                MDC.put(CLUSTER_NAME_KEY, clusterKey);

                List<String> allTopics = new ArrayList<>();
                allTopics.addAll(generateTopicNames("consumer", config.consumerGroupCount()));
                allTopics.addAll(generateTopicNames("share", config.shareGroupCount()));
                allTopics.addAll(generateTopicNames("streams", config.streamsGroupCount()));

                initializeCounts(clusterKey, allTopics);

                Admin adminClient = Admin.create(configProperties);
                adminClients.put(clusterKey, adminClient);

                initialize(clusterKey, adminClient, allTopics, config.partitionsPerTopic());

                if (config.consumerGroupCount() > 0) {
                    startGroups("consumer",
                            config.consumerGroupCount(),
                            clusterKey,
                            clientCount,
                            (props, _) -> new KafkaConsumer<byte[], byte[]>(props),
                            KafkaConsumer::subscribe,
                            KafkaConsumer::poll);
                }

                if (config.shareGroupCount() > 0) {
                    startGroups("share",
                            config.shareGroupCount(),
                            clusterKey,
                            clientCount,
                            (props, _) -> new KafkaShareConsumer<byte[], byte[]>(props),
                            KafkaShareConsumer::subscribe,
                            KafkaShareConsumer::poll);
                }

                if (config.streamsGroupCount() > 0) {
                    startGroups("streams",
                            config.streamsGroupCount(),
                            clusterKey,
                            clientCount,
                            (props, topics) -> createStreams(clusterKey, props, topics),
                            (_, _) -> { /* Subscribe for KafkaStreams is no-op */ },
                            (_, duration) -> {
                                LockSupport.parkNanos(duration.toNanos());
                                return ConsumerRecords.EMPTY;
                            } /* Poll for KafkaStreams is no-op */);
                }
            }));
    }

    List<String> generateTopicNames(String type, int count) {
        return IntStream.range(0, count)
            .mapToObj(groupNumber -> generateGroupTopicNames(type, groupNumber))
            .flatMap(Function.identity())
            .toList();
    }

    Stream<String> generateGroupTopicNames(String type, int groupNumber) {
        return IntStream
                .range(0, config.topicsPerMember())
                // shift by 10 for the suffix to match previous behavior
                .mapToObj(t -> Integer.toString(t + 10, 36).toLowerCase(Locale.ROOT))
                .map(suffix -> config.topicPattern().formatted(groupNumber, type + '-' + suffix));
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

    void initializeCounts(String clusterKey, List<String> topics) {
        Map<TopicPartition, Long> initialCounts = topics.stream()
                .flatMap(topic -> IntStream
                        .range(0, config.partitionsPerTopic())
                        .mapToObj(p -> new TopicPartition(topic, p)))
                .map(topicPartition -> Map.entry(topicPartition, 0L))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        recordsProduced.put(clusterKey, new ConcurrentHashMap<>(initialCounts));
        recordsConsumed.put(clusterKey, new ConcurrentHashMap<>(initialCounts));
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

        Map<GroupType, List<String>> dataGenGroups = adminClient.listGroups(new ListGroupsOptions()
                .inGroupStates(Set.of(GroupState.EMPTY)))
            .all()
            .toCompletionStage()
            .toCompletableFuture()
            .join()
            .stream()
            .filter(group -> group.groupId().startsWith("console-datagen-group-"))
            .collect(groupingBy(
                    group -> group.type().orElse(GroupType.UNKNOWN),
                    mapping(GroupListing::groupId, toList())));

        log.infof("Deleting existing consumer groups %s", dataGenGroups);

        dataGenGroups.entrySet()
            .stream()
            .<Map<String, KafkaFuture<Void>>>map(entry -> {
                switch (entry.getKey()) {
                    case CLASSIC, CONSUMER:
                        return adminClient.deleteConsumerGroups(entry.getValue())
                                .deletedGroups();
                    case SHARE:
                        return adminClient.deleteShareGroups(entry.getValue())
                                .deletedGroups();
                    case STREAMS:
                        return adminClient.deleteStreamsGroups(entry.getValue())
                                .deletedGroups();
                    default:
                        return java.util.Collections.emptyMap();
                }
            })
            .flatMap(deleted -> deleted.entrySet().stream())
            .map(e -> e.getValue().toCompletionStage().exceptionally(error -> {
                  error = causeIfCompletionException(error);

                  if (error instanceof GroupNotEmptyException) {
                      log.warnf("Group %s is not empty and cannot be deleted", e.getKey());
                  } else if (error instanceof GroupIdNotFoundException) {
                      // Ignore
                  } else {
                      log.warnf(error, "Error deleting group %s: %s", e.getKey(), error.getMessage());
                  }
                  return null;
            }))
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll())
            .join();

        log.infof("Deleting existing topics %s", topics);
        Set<String> remainingTopics = new HashSet<>();
        int deleteTopicsMax = 10;

        do {
            remainingTopics.clear();

            adminClient.deleteTopics(topics)
                .topicNameValues()
                .entrySet()
                .stream()
                .map(e -> e.getValue().toCompletionStage().exceptionally(error -> {
                    error = causeIfCompletionException(error);

                    if (!(error instanceof UnknownTopicOrPartitionException)) {
                        remainingTopics.add(e.getKey());
                        log.warnf(error, "Error deleting topic %s: %s", e.getKey(), error.getMessage());
                    }

                    return null;
                }))
                .map(CompletionStage::toCompletableFuture)
                .collect(awaitingAll())
                .thenRun(() -> LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5)))
                .join();
        } while (--deleteTopicsMax > 0 && !remainingTopics.isEmpty());

        var newTopics = topics.stream()
                .map(t -> new NewTopic(t, Optional.of(partitionsPerTopic), config.topicReplicationFactor())
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
                error = causeIfCompletionException(error);
                log.warnf(error, "Error creating topic %s: %s", e.getKey(), error.getMessage());
                return null;
            }))
            .map(CompletionStage::toCompletableFuture)
            .collect(awaitingAll())
            .thenRun(() -> LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5)))
            .thenRun(() -> log.infof("Topics created: %s", topics))
            .join();
    }

    <C extends AutoCloseable> void startGroups(String type, int count, String clusterKey, AtomicInteger clientCount,
            BiFunction<Map<String, Object>, List<String>, C> factory,
            BiConsumer<C, Collection<String>> subscribe,
            BiFunction<C, Duration, ConsumerRecords<?, ?>> poll) {

        for (int g = 0; g < count; g++) {
            var groupNumber = g;
            var topics = generateGroupTopicNames(type, groupNumber).toList();

            virtualExec.submit(() -> {
                var configs = new HashMap<>(producerConfigs.get(clusterKey));
                String clientId = "console-datagen-producer-%s-%d-%d".formatted(type, groupNumber, clientCount.incrementAndGet());
                configs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

                String compressionType = compressionTypes.get(groupNumber % compressionTypes.size());
                configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);

                MDC.put(CLUSTER_NAME_KEY, clusterKey);
                MDC.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

                try (Producer<byte[], byte[]> producer = new KafkaProducer<>(configs)) {
                    log.infof("Created producer %s with compression %s", clientId, compressionType);
                    while (running.get()) {
                        produce(clusterKey, producer, topics);
                    }
                } catch (Exception e) {
                    log.warnf(e, "Error producing: %s", e.getMessage());
                }

                log.infof("Run loop complete for producer %d on cluster %s", groupNumber, clusterKey);
            });

            String groupId = "console-datagen-group-%s-%d".formatted(type, groupNumber);

            for (int m = 0; m < config.membersPerGroup(); m++) {
                virtualExec.submit(() -> {
                    var configs = new HashMap<>(consumerConfigs.get(clusterKey));
                    configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

                    String clientId = "console-datagen-consumer-%s-%d-%d".formatted(type, groupNumber, clientCount.incrementAndGet());
                    configs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

                    MDC.put(CLUSTER_NAME_KEY, clusterKey);
                    MDC.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

                    try (C consumer = factory.apply(configs, topics)) {
                        log.infof("Created consumer %s", clientId);
                        subscribe.accept(consumer, topics);

                        while (running.get()) {
                            poll.apply(consumer, Duration.ofSeconds(2))
                                .forEach(rec -> consume(clusterKey, rec));
                        }
                    } catch (Exception e) {
                        log.warnf(e, "Error consuming: %s", e.getMessage());
                    }

                    log.infof("Run loop complete for group %s; client %s", groupId, clientId);
                });
            }
        }
    }

    private KafkaStreams createStreams(String clusterKey, Map<String, Object> props, List<String> topics) {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, props.get(ConsumerConfig.GROUP_ID_CONFIG));
        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupType.STREAMS.name().toLowerCase(Locale.ROOT));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(topics);
        stream.process(() -> new org.apache.kafka.streams.processor.api.Processor<String, String, Void, Void>() {
            private org.apache.kafka.streams.processor.api.ProcessorContext<Void, Void> context;

            @Override
            public void init(final ProcessorContext<Void, Void> context) {
                this.context = context;
            }

            @Override
            public void process(org.apache.kafka.streams.processor.api.Record<String, String> streamRec) {
                var meta = context.recordMetadata().orElseThrow();
                var rec = new ConsumerRecord<String, String>(
                        meta.topic(),
                        meta.partition(),
                        meta.offset(),
                        streamRec.key(),
                        streamRec.value()
                );

                consume(clusterKey, rec);
            }
        });

        Properties properties = new Properties();
        props.forEach(properties::put);
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
        return streams;
    }

    Throwable causeIfCompletionException(Throwable thrown) {
        return thrown instanceof CompletionException ? thrown.getCause() : thrown;
    }

    void produce(String clusterKey, Producer<byte[], byte[]> producer, List<String> topics) {
        byte[] buffer = new byte[500];
        int t = 0;
        long start = System.currentTimeMillis();
        long rate = 100 * ((start / 10000) % 5) + 10;

        for (int i = 0; i < rate; i++) {
            if (!running.get()) {
                return;
            }

            generator.nextBytes(buffer);

            Function<Beer, JsonObject> beerBuilder = beer ->
                json.createObjectBuilder()
                    .add("name", beer.name())
                    .add("style", beer.style())
                    .build();

            byte[] key = json.createObjectBuilder()
                    .add("storeId", faker.idNumber().valid())
                    .add("operatorId", faker.idNumber().valid())
                    .add("messageId", faker.idNumber().valid())
                    .build()
                    .toString()
                    .getBytes();

            byte[] value = json.createObjectBuilder()
                    .add("timestamp", Instant.now().toString())
                    .add("user", json.createObjectBuilder()
                        .add("lastName", faker.name().lastName())
                        .add("firstName", faker.name().firstName())
                        .add("birthDate", faker.date().birthday().toInstant().toString())
                        .add("address", json.createObjectBuilder()
                            .add("number", faker.address().streetAddressNumber())
                            .add("street", faker.address().streetName())
                            .add("city", faker.address().cityName())
                            .add("region", faker.address().state())
                            .add("postalCode", faker.address().zipCode())
                        )
                        .add("favoriteBeers", json.createArrayBuilder()
                            .add(beerBuilder.apply(faker.beer()))
                            .add(beerBuilder.apply(faker.beer()))
                        )
                    )
                    .add("payload", Base64.getEncoder().encodeToString(buffer))
                    .build()
                    .toString()
                    .getBytes();



            String topic = topics.get(t++ % topics.size());
            var producerRecord = new ProducerRecord<>(topic, key, value);
            producerRecord.headers().add("X-Country", faker.country().name().getBytes());
            producerRecord.headers().add("X-Animal", faker.animal().name().getBytes());

            complete(producer.send(producerRecord))
                .thenAccept(meta -> {
                    TopicPartition topicPartition = new TopicPartition(meta.topic(), meta.partition());
                    var currentCount = incrementAndGet(recordsProduced, clusterKey, topicPartition);

                    if (currentCount % 5_000 == 0) {
                        log.infof("Produced %d records to %s (since startup)", currentCount, topicPartition);
                    }
                })
                .exceptionally(error -> {
                    error = causeIfCompletionException(error);
                    log.warnf("Error producing record: %s - %s", error.getClass().getName(), error.getMessage());
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

    void consume(String clusterKey, ConsumerRecord<?, ?> rec) {
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
            .compute(topicPartition, (_, v) -> v == null ? 1 : v + 1);
    }

    void maybeDeleteRecords(Admin adminClient, TopicPartition topicPartition, Long offset) {
        var earliest = getOffset(adminClient, topicPartition, OffsetSpec.earliest());
        var latest = getOffset(adminClient, topicPartition, OffsetSpec.latest());

        CompletableFuture.allOf(earliest, latest)
            .thenComposeAsync(_ -> {
                long diff = latest.join() - earliest.join();

                if (diff >= config.maxTopicDepth()) {
                    log.infof("Offset diff is %d, truncating partition %s to offset %d",
                            diff, topicPartition, offset);
                    // Truncate the topic to the up to the previous offset
                    var recordsToDelete = Map.of(topicPartition, RecordsToDelete.beforeOffset(offset));
                    return adminClient.deleteRecords(recordsToDelete)
                        .all()
                        .toCompletionStage();
                } else {
                    log.debugf("Offset diff is %d for partition %s at offset %d", diff, topicPartition, offset);
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
