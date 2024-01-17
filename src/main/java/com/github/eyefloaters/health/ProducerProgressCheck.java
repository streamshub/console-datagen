package com.github.eyefloaters.health;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;

@Liveness
@ApplicationScoped
public class ProducerProgressCheck extends CounterProgressCheck implements HealthCheck {

    @Inject
    @Named("recordsProduced")
    Map<String, Map<TopicPartition, Long>> recordsProduced;

    public ProducerProgressCheck() {
        super("producer-progress");
    }

    @Override
    public HealthCheckResponse call() {
        return check(recordsProduced);
    }
}
