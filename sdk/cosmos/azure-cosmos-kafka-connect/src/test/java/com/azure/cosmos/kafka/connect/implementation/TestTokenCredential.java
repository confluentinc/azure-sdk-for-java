// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

/**
 * Mock TokenCredential implementation for testing custom authentication
 */
public class TestTokenCredential implements TokenCredential {
    private static final String DEFAULT_TOKEN = "test-token-12345";
    private static final OffsetDateTime DEFAULT_EXPIRY = OffsetDateTime.now().plusHours(1);
    
    private final String token;
    private final OffsetDateTime expiresAt;
    
    public TestTokenCredential() {
        this(DEFAULT_TOKEN, DEFAULT_EXPIRY);
    }
    
    public TestTokenCredential(String token, OffsetDateTime expiresAt) {
        this.token = token;
        this.expiresAt = expiresAt;
    }
    
    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
        return Mono.just(new AccessToken(token, expiresAt));
    }
}
