package com.github.eyefloaters;

import java.util.HashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
public class AdminFactory {

    @Inject
    @Identifier("default-kafka-broker")
    Map<String, Object> config;

    @Produces
    Admin getAdmin() {
        Map<String, Object> copy = new HashMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (AdminClientConfig.configNames().contains(entry.getKey())) {
                copy.put(entry.getKey(), entry.getValue());
            }
        }
        return Admin.create(copy);
    }

}
