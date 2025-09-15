// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import org.apache.kafka.common.Configurable;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Mock TokenCredential implementation that implements Configurable for testing custom authentication
 */
public class ConfigurableTestTokenCredential implements TokenCredential, Configurable {
    private static final String DEFAULT_TOKEN = "configurable-test-token-67890";
    
    private String configuredToken = DEFAULT_TOKEN;
    private OffsetDateTime expiresAt = OffsetDateTime.now().plusHours(1);
    private Map<String, ?> configuration;
    
    public ConfigurableTestTokenCredential() {
        // Default constructor required
    }
    
    @Override
    public void configure(Map<String, ?> configs) {
        this.configuration = configs;
        if (configs.containsKey("token")) {
            this.configuredToken = configs.get("token").toString();
        }
        if (configs.containsKey("expiryMinutes")) {
            int expiryMinutes = Integer.parseInt(configs.get("expiryMinutes").toString());
            this.expiresAt = OffsetDateTime.now().plusMinutes(expiryMinutes);
        }
    }
    
    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
        return Mono.just(new AccessToken(configuredToken, expiresAt));
    }
    
    public Map<String, ?> getConfiguration() {
        return configuration;
    }
    
    public String getConfiguredToken() {
        return configuredToken;
    }
}
