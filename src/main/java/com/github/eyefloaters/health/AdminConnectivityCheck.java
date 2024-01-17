package com.github.eyefloaters.health;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.kafka.clients.admin.Admin;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;

@Liveness
@ApplicationScoped
public class AdminConnectivityCheck implements HealthCheck {

    @Inject
    @Named("adminClients")
    Map<String, Admin> adminClients;

    @Override
    public HealthCheckResponse call() {
        var builder = HealthCheckResponse.builder().name("generator-liveness");
        boolean up = true;

        long configuredClusters = adminClients.size();
        long availableClusters = adminClients.values()
            .stream()
            .map(client -> client.describeCluster()
                .clusterId()
                .toCompletionStage()
                .thenApply(clusterId -> true)
                .exceptionally(error -> false)
                .toCompletableFuture()
                .join())
            .filter(Boolean.TRUE::equals)
            .count();

        builder.withData("configuredClusters", configuredClusters);
        builder.withData("availableClusters", availableClusters);

        if (availableClusters < configuredClusters) {
            up = false;
        }

        builder.status(up);

        return builder.build();
    }

}
