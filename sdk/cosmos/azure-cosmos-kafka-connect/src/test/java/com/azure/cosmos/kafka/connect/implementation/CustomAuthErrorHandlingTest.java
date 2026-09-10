// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

import com.azure.cosmos.implementation.TestConfigurations;
import com.azure.cosmos.kafka.connect.CosmosSinkConnector;
import com.azure.cosmos.kafka.connect.CosmosSourceConnector;
import com.azure.cosmos.kafka.connect.InMemoryStorageReader;
import com.azure.cosmos.kafka.connect.KafkaCosmosReflectionUtils;
import com.azure.cosmos.kafka.connect.KafkaCosmosTestSuiteBase;
import com.azure.cosmos.kafka.connect.implementation.sink.CosmosSinkTask;
import com.azure.cosmos.kafka.connect.implementation.source.CosmosSourceTask;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.testng.Assert.assertThrows;

public class CustomAuthErrorHandlingTest extends KafkaCosmosTestSuiteBase {

    @Test(groups = { "unit" })
    public void sinkTaskFailsWithInvalidCredentialProviderClass() {
        String topicName = singlePartitionContainerName;

        Map<String, String> sinkConfigMap = new HashMap<>();
        sinkConfigMap.put("azure.cosmos.account.endpoint", TestConfigurations.HOST);
        sinkConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sinkConfigMap.put("credentials.provider.class", InvalidTestTokenCredential.class.getName());
        sinkConfigMap.put("azure.cosmos.sink.database.name", databaseName);
        sinkConfigMap.put("azure.cosmos.sink.containers.topicMap", topicName + "#" + singlePartitionContainerName);
        sinkConfigMap.put("azure.cosmos.sink.task.id", UUID.randomUUID().toString());

        CosmosSinkTask sinkTask = new CosmosSinkTask();
        SinkTaskContext sinkTaskContext = Mockito.mock(SinkTaskContext.class);
        Mockito.when(sinkTaskContext.errantRecordReporter()).thenReturn(null);
        KafkaCosmosReflectionUtils.setSinkTaskContext(sinkTask, sinkTaskContext);

        // Should throw IllegalArgumentException when trying to start with invalid credential provider
        try {
            sinkTask.start(sinkConfigMap);
            // Should not reach here
            assertThat(false).isTrue();
        } catch (IllegalArgumentException exception) {
            assertThat(exception.getMessage()).contains("cannot be cast to TokenCredential");
            assertThat(exception.getMessage()).contains("doesn't implement TokenCredential interface");
        }
    }

    @Test(groups = { "unit" })
    public void sinkTaskFailsWithNonExistentCredentialProviderClass() {
        String topicName = singlePartitionContainerName;

        Map<String, String> sinkConfigMap = new HashMap<>();
        sinkConfigMap.put("azure.cosmos.account.endpoint", TestConfigurations.HOST);
        sinkConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sinkConfigMap.put("credentials.provider.class", "com.nonexistent.FakeTokenCredential");
        sinkConfigMap.put("azure.cosmos.sink.database.name", databaseName);
        sinkConfigMap.put("azure.cosmos.sink.containers.topicMap", topicName + "#" + singlePartitionContainerName);
        sinkConfigMap.put("azure.cosmos.sink.task.id", UUID.randomUUID().toString());

        CosmosSinkTask sinkTask = new CosmosSinkTask();
        SinkTaskContext sinkTaskContext = Mockito.mock(SinkTaskContext.class);
        Mockito.when(sinkTaskContext.errantRecordReporter()).thenReturn(null);
        KafkaCosmosReflectionUtils.setSinkTaskContext(sinkTask, sinkTaskContext);

        // Should throw IllegalArgumentException when trying to start with non-existent credential provider
        try {
            sinkTask.start(sinkConfigMap);
            // Should not reach here
            assertThat(false).isTrue();
        } catch (IllegalArgumentException exception) {
            assertThat(exception.getMessage()).startsWith("Credential provider class not found:");
            assertThat(exception.getCause()).isInstanceOf(ClassNotFoundException.class);
        }
    }

    @Test(groups = { "unit" })
    public void sinkTaskFailsWithCredentialProviderWithoutDefaultConstructor() {
        String topicName = singlePartitionContainerName;

        Map<String, String> sinkConfigMap = new HashMap<>();
        sinkConfigMap.put("azure.cosmos.account.endpoint", TestConfigurations.HOST);
        sinkConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sinkConfigMap.put("credentials.provider.class", NoDefaultConstructorTokenCredential.class.getName());
        sinkConfigMap.put("azure.cosmos.sink.database.name", databaseName);
        sinkConfigMap.put("azure.cosmos.sink.containers.topicMap", topicName + "#" + singlePartitionContainerName);
        sinkConfigMap.put("azure.cosmos.sink.task.id", UUID.randomUUID().toString());

        CosmosSinkTask sinkTask = new CosmosSinkTask();
        SinkTaskContext sinkTaskContext = Mockito.mock(SinkTaskContext.class);
        Mockito.when(sinkTaskContext.errantRecordReporter()).thenReturn(null);
        KafkaCosmosReflectionUtils.setSinkTaskContext(sinkTask, sinkTaskContext);

        // Should throw IllegalArgumentException when trying to start with credential provider without default constructor
        try {
            sinkTask.start(sinkConfigMap);
            // Should not reach here
            assertThat(false).isTrue();
        } catch (IllegalArgumentException exception) {
            assertThat(exception.getMessage()).contains("must have a public no-argument constructor");
            assertThat(exception.getCause()).isInstanceOf(NoSuchMethodException.class);
        }
    }

    @Test(groups = { "unit" })
    public void sinkTaskFailsWithEmptyCredentialProviderClass() {
        String topicName = singlePartitionContainerName;

        Map<String, String> sinkConfigMap = new HashMap<>();
        sinkConfigMap.put("azure.cosmos.account.endpoint", TestConfigurations.HOST);
        sinkConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sinkConfigMap.put("credentials.provider.class", ""); // Empty class name
        sinkConfigMap.put("azure.cosmos.sink.database.name", databaseName);
        sinkConfigMap.put("azure.cosmos.sink.containers.topicMap", topicName + "#" + singlePartitionContainerName);
        sinkConfigMap.put("azure.cosmos.sink.task.id", UUID.randomUUID().toString());

        CosmosSinkTask sinkTask = new CosmosSinkTask();
        SinkTaskContext sinkTaskContext = Mockito.mock(SinkTaskContext.class);
        Mockito.when(sinkTaskContext.errantRecordReporter()).thenReturn(null);
        KafkaCosmosReflectionUtils.setSinkTaskContext(sinkTask, sinkTaskContext);

        // Should throw IllegalArgumentException when trying to start with empty credential provider class
        try {
            sinkTask.start(sinkConfigMap);
            // Should not reach here
            assertThat(false).isTrue();
        } catch (IllegalArgumentException exception) {
            assertThat(exception.getMessage()).isEqualTo("Credential provider class is required for Custom authentication");
        }
    }

    @Test(groups = { "unit" })
    public void sinkTaskDoesNotLeakCredentialProviderExceptionMessage() {
        String topicName = singlePartitionContainerName;

        Map<String, String> sinkConfigMap = new HashMap<>();
        sinkConfigMap.put("azure.cosmos.account.endpoint", TestConfigurations.HOST);
        sinkConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sinkConfigMap.put("credentials.provider.class", ThrowingConfigurableTokenCredential.class.getName());
        sinkConfigMap.put("azure.cosmos.sink.database.name", databaseName);
        sinkConfigMap.put("azure.cosmos.sink.containers.topicMap", topicName + "#" + singlePartitionContainerName);
        sinkConfigMap.put("azure.cosmos.sink.task.id", UUID.randomUUID().toString());

        CosmosSinkTask sinkTask = new CosmosSinkTask();
        SinkTaskContext sinkTaskContext = Mockito.mock(SinkTaskContext.class);
        Mockito.when(sinkTaskContext.errantRecordReporter()).thenReturn(null);
        KafkaCosmosReflectionUtils.setSinkTaskContext(sinkTask, sinkTaskContext);

        // The provider's configure() throws with a canary; the connector must not surface that message or
        // carry the provider exception as a cause (connector/task-start logs the whole throwable at WARN).
        try {
            sinkTask.start(sinkConfigMap);
            // Should not reach here
            assertThat(false).isTrue();
        } catch (IllegalArgumentException exception) {
            assertThat(exception.getMessage()).startsWith("Failed to create credential provider:");
            assertThat(exception.getCause()).isNull();
            for (Throwable current = exception; current != null; current = current.getCause()) {
                String message = current.getMessage() == null ? "" : current.getMessage();
                assertThat(message).doesNotContain(ThrowingConfigurableTokenCredential.FAILURE_MESSAGE);
            }
        }
    }

    @Test(groups = { "unit" })
    public void sourceTaskFailsWithInvalidCredentialProviderClass() {
        Map<String, String> sourceConfigMap = new HashMap<>();
        sourceConfigMap.put("azure.cosmos.account.endpoint", TestConfigurations.HOST);
        sourceConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sourceConfigMap.put("credentials.provider.class", InvalidTestTokenCredential.class.getName());
        sourceConfigMap.put("azure.cosmos.source.database.name", databaseName);
        sourceConfigMap.put("azure.cosmos.source.containers.includedList", Arrays.asList(singlePartitionContainerName).toString());
        sourceConfigMap.put("azure.cosmos.source.task.id", UUID.randomUUID().toString());
        sourceConfigMap.put("azure.cosmos.source.task.feedRangeTaskUnits", "[]"); // Empty but valid JSON array

        CosmosSourceTask sourceTask = new CosmosSourceTask();
        sourceTask.initialize(new TestSourceTaskContext(sourceConfigMap));

        // Should throw IllegalArgumentException when trying to start with invalid credential provider
        try {
            sourceTask.start(sourceConfigMap);
            // Should not reach here
            assertThat(false).isTrue();
        } catch (IllegalArgumentException exception) {
            assertThat(exception.getMessage()).contains("cannot be cast to TokenCredential");
        }
    }

    @Test(groups = { "unit" })
    public void sourceTaskFailsWithNonExistentCredentialProviderClass() {
        Map<String, String> sourceConfigMap = new HashMap<>();
        sourceConfigMap.put("azure.cosmos.account.endpoint", TestConfigurations.HOST);
        sourceConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sourceConfigMap.put("credentials.provider.class", "com.nonexistent.FakeTokenCredential");
        sourceConfigMap.put("azure.cosmos.source.database.name", databaseName);
        sourceConfigMap.put("azure.cosmos.source.containers.includedList", Arrays.asList(singlePartitionContainerName).toString());
        sourceConfigMap.put("azure.cosmos.source.task.id", UUID.randomUUID().toString());
        sourceConfigMap.put("azure.cosmos.source.task.feedRangeTaskUnits", "[]"); // Empty but valid JSON array

        CosmosSourceTask sourceTask = new CosmosSourceTask();
        sourceTask.initialize(new TestSourceTaskContext(sourceConfigMap));

        // Should throw IllegalArgumentException when trying to start with non-existent credential provider
        try {
            sourceTask.start(sourceConfigMap);
            // Should not reach here
            assertThat(false).isTrue();
        } catch (IllegalArgumentException exception) {
            assertThat(exception.getMessage()).startsWith("Credential provider class not found:");
            assertThat(exception.getCause()).isInstanceOf(ClassNotFoundException.class);
        }
    }

    @Test(groups = { "unit" })
    public void connectorValidationFailsForCustomAuthWithoutCredentialProvider() {
        CosmosSinkConnector sinkConnector = new CosmosSinkConnector();

        Map<String, String> sinkConfigMap = new HashMap<>();
        sinkConfigMap.put("azure.cosmos.account.endpoint", TestConfigurations.HOST);
        sinkConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        // Missing credentials.provider.class
        sinkConfigMap.put("azure.cosmos.sink.database.name", "testDatabase");
        sinkConfigMap.put("azure.cosmos.sink.containers.topicMap", "topic#container");

        Config config = sinkConnector.validate(sinkConfigMap);
        boolean hasErrors = config.configValues().stream()
            .anyMatch(configValue -> !configValue.errorMessages().isEmpty());

        assertThat(hasErrors).isTrue();
    }

    @Test(groups = { "unit" })
    public void sourceConnectorValidationFailsForCustomAuthWithoutCredentialProvider() {
        CosmosSourceConnector sourceConnector = new CosmosSourceConnector();

        Map<String, String> sourceConfigMap = new HashMap<>();
        sourceConfigMap.put("azure.cosmos.account.endpoint", TestConfigurations.HOST);
        sourceConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        // Missing credentials.provider.class
        sourceConfigMap.put("azure.cosmos.source.database.name", "testDatabase");
        sourceConfigMap.put("azure.cosmos.source.containers.includedList", Arrays.asList("container1").toString());

        Config config = sourceConnector.validate(sourceConfigMap);
        boolean hasErrors = config.configValues().stream()
            .anyMatch(configValue -> !configValue.errorMessages().isEmpty());

        assertThat(hasErrors).isTrue();
    }

    private static class TestSourceTaskContext implements SourceTaskContext {
        private final Map<String, String> map;
        private final OffsetStorageReader offsetStorageReader;

        public TestSourceTaskContext(Map<String, String> map) {
            this.map = map;
            this.offsetStorageReader = new InMemoryStorageReader();
        }

        @Override
        public Map<String, String> configs() {
            return this.map;
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            return this.offsetStorageReader;
        }
    }
}
