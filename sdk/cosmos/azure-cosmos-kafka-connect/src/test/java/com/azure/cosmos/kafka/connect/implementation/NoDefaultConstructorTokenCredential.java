// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

/**
 * Mock TokenCredential implementation without default constructor for testing error scenarios
 */
public class NoDefaultConstructorTokenCredential implements TokenCredential {
    
    private final String requiredParameter;
    
    // Only parameterized constructor - no default constructor
    public NoDefaultConstructorTokenCredential(String requiredParameter) {
        this.requiredParameter = requiredParameter;
    }
    
    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
        return Mono.just(new AccessToken("no-default-constructor-token", OffsetDateTime.now().plusHours(1)));
    }
}
