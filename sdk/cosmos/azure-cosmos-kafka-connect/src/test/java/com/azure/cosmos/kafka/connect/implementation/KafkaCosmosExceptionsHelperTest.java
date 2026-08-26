// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

import com.azure.cosmos.CosmosException;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract test for {@link KafkaCosmosExceptionsHelper#getSafeExceptionDiagnostics(Throwable)}, the shared
 * sanitizer used by the CosmosWriterBase / CosmosBulkWriter / CosmosPointWriter write-path log sites. It must
 * never surface a {@link CosmosException}'s {@code resourceAddress} (rendered by {@code toString()}), only the
 * status code, sub-status code and activity id.
 */
public class KafkaCosmosExceptionsHelperTest {

    @Test(groups = { "unit" })
    public void cosmosExceptionDiagnosticsExcludeResourceAddress() {
        String canaryResourceAddress = "dbs/db/colls/coll/docs/secret-record-id-canary-9f3a2b";
        CosmosException cosmosException = new CanaryCosmosException(canaryResourceAddress, 429);

        String diagnostics = KafkaCosmosExceptionsHelper.getSafeExceptionDiagnostics(cosmosException);

        assertThat(diagnostics).doesNotContain(canaryResourceAddress);
        assertThat(diagnostics).contains("statusCode: 429");
        assertThat(diagnostics).contains("subStatusCode");
        assertThat(diagnostics).contains("activityId");
        // The raw toString() (which does carry resourceAddress) must not be what we emit.
        assertThat(diagnostics).isNotEqualTo(cosmosException.toString());
    }

    @Test(groups = { "unit" })
    public void nonCosmosExceptionFallsBackToClassAndMessage() {
        String diagnostics =
            KafkaCosmosExceptionsHelper.getSafeExceptionDiagnostics(new IllegalArgumentException("bad map"));

        assertThat(diagnostics).contains("IllegalArgumentException");
        assertThat(diagnostics).contains("bad map");
    }

    private static final class CanaryCosmosException extends CosmosException {
        CanaryCosmosException(String resourceAddress, int statusCode) {
            super(resourceAddress, statusCode, null, null);
        }
    }
}
