// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import org.apache.kafka.common.Configurable;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Mock TokenCredential whose {@link #configure(Map)} throws with a recognizable message, used to verify the
 * connector does not surface a custom credential provider's exception message (or cause) when instantiation
 * fails. The provider receives the auth config map, so its own error text is outside the connector's control.
 */
public class ThrowingConfigurableTokenCredential implements TokenCredential, Configurable {
    // Synthetic stand-in for a value a provider's own error text might echo (e.g. a secret from the config map).
    public static final String FAILURE_MESSAGE = "provider-configure-failure-canary-9f3a2b";

    public ThrowingConfigurableTokenCredential() {
        // Default constructor required
    }

    @Override
    public void configure(Map<String, ?> configs) {
        throw new IllegalStateException(FAILURE_MESSAGE);
    }

    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
        return Mono.empty();
    }
}
