package com.github.streamshub;

import java.util.Map;
import java.util.Optional;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "datagen")
public interface DataGenConfig {

    boolean enabled();

    int consumerGroupCount();

    int shareGroupCount();

    int topicsPerMember();

    int partitionsPerTopic();

    Optional<Short> topicReplicationFactor();

    String topicPattern();

    int maxTopicDepth();

    DataGenConfig.Security security();

    Map<String, DataGenConfig.KafkaConfig> kafka();

    interface Security {
        boolean trustCertificates();
    }

    interface KafkaConfig {
        String name();
        Map<String, String> configs();
    }
}
