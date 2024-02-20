package com.github.eyefloaters;

import java.util.Map;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "datagen")
public interface DataGenConfig {

    boolean enabled();

    String topicPattern();

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
