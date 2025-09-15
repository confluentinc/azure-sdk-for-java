// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.implementation.TestConfigurations;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.kafka.connect.CosmosSinkConnector;
import com.azure.cosmos.kafka.connect.CosmosSourceConnector;
import com.azure.cosmos.kafka.connect.KafkaCosmosReflectionUtils;
import com.azure.cosmos.kafka.connect.KafkaCosmosTestConfigurations;
import com.azure.cosmos.kafka.connect.KafkaCosmosTestSuiteBase;
import com.azure.cosmos.kafka.connect.TestItem;
import com.azure.cosmos.kafka.connect.implementation.sink.CosmosSinkTask;
import com.azure.cosmos.kafka.connect.implementation.source.CosmosSourceTask;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class CustomAuthIntegrationTest extends KafkaCosmosTestSuiteBase {

    @Test(groups = { "kafka", "kafka-emulator" }, timeOut = TIMEOUT)
    public void sinkTaskWithCustomAuthentication() {
        String topicName = singlePartitionContainerName;

        Map<String, String> sinkConfigMap = new HashMap<>();
        sinkConfigMap.put("azure.cosmos.account.endpoint", TestConfigurations.HOST);
        // Use CUSTOM auth type with our test credential provider
        sinkConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sinkConfigMap.put("credentials.provider.class", TestTokenCredential.class.getName());
        sinkConfigMap.put("azure.cosmos.sink.database.name", databaseName);
        sinkConfigMap.put("azure.cosmos.sink.containers.topicMap", topicName + "#" + singlePartitionContainerName);
        sinkConfigMap.put("azure.cosmos.sink.bulk.enabled", "true");
        sinkConfigMap.put("azure.cosmos.sink.task.id", UUID.randomUUID().toString());

        CosmosSinkTask sinkTask = new CosmosSinkTask();
        SinkTaskContext sinkTaskContext = Mockito.mock(SinkTaskContext.class);
        Mockito.when(sinkTaskContext.errantRecordReporter()).thenReturn(null);
        KafkaCosmosReflectionUtils.setSinkTaskContext(sinkTask, sinkTaskContext);

        try {
            sinkTask.start(sinkConfigMap);

            CosmosAsyncClient cosmosClient = KafkaCosmosReflectionUtils.getSinkTaskCosmosClient(sinkTask);
            CosmosContainerProperties singlePartitionContainerProperties = getSinglePartitionContainer(cosmosClient);
            CosmosAsyncContainer container = cosmosClient.getDatabase(databaseName).getContainer(singlePartitionContainerProperties.getId());

            List<SinkRecord> sinkRecordList = new ArrayList<>();
            List<TestItem> toBeCreateItems = new ArrayList<>();

            // Create test records
            for (int i = 0; i < 5; i++) {
                TestItem testItem = TestItem.createNewItem();
                toBeCreateItems.add(testItem);

                SinkRecord sinkRecord = new SinkRecord(
                    topicName,
                    1,
                    new ConnectSchema(Schema.Type.STRING),
                    testItem.getId(),
                    new ConnectSchema(Schema.Type.MAP),
                    Utils.getSimpleObjectMapper().convertValue(
                        Utils.getSimpleObjectMapper().convertValue(testItem, ObjectNode.class),
                        new TypeReference<Map<String, Object>>() {}),
                    0L);
                sinkRecordList.add(sinkRecord);
            }

            sinkTask.put(sinkRecordList);

            // Verify items were written
            List<String> writtenItemIds = new ArrayList<>();
            String query = "select * from c";
            container.queryItems(query, TestItem.class)
                .byPage()
                .flatMap(response -> {
                    writtenItemIds.addAll(
                        response.getResults().stream().map(TestItem::getId).collect(Collectors.toList()));
                    return Mono.empty();
                })
                .blockLast();

            assertThat(writtenItemIds.size()).isEqualTo(toBeCreateItems.size());
            List<String> toBeCreateItemIds = toBeCreateItems.stream().map(TestItem::getId).collect(Collectors.toList());
            assertThat(writtenItemIds.containsAll(toBeCreateItemIds)).isTrue();

        } finally {
            sinkTask.stop();
        }
    }

    @Test(groups = { "kafka", "kafka-emulator" }, timeOut = TIMEOUT)
    public void sinkTaskWithConfigurableCustomAuthentication() {
        String topicName = singlePartitionContainerName;

        Map<String, String> sinkConfigMap = new HashMap<>();
        sinkConfigMap.put("azure.cosmos.account.endpoint", TestConfigurations.HOST);
        // Use CUSTOM auth type with configurable test credential provider
        sinkConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sinkConfigMap.put("credentials.provider.class", ConfigurableTestTokenCredential.class.getName());
        sinkConfigMap.put("token", "custom-integration-token");
        sinkConfigMap.put("expiryMinutes", "60");
        sinkConfigMap.put("azure.cosmos.sink.database.name", databaseName);
        sinkConfigMap.put("azure.cosmos.sink.containers.topicMap", topicName + "#" + singlePartitionContainerName);
        sinkConfigMap.put("azure.cosmos.sink.bulk.enabled", "false");
        sinkConfigMap.put("azure.cosmos.sink.task.id", UUID.randomUUID().toString());

        CosmosSinkTask sinkTask = new CosmosSinkTask();
        SinkTaskContext sinkTaskContext = Mockito.mock(SinkTaskContext.class);
        Mockito.when(sinkTaskContext.errantRecordReporter()).thenReturn(null);
        KafkaCosmosReflectionUtils.setSinkTaskContext(sinkTask, sinkTaskContext);

        try {
            sinkTask.start(sinkConfigMap);

            CosmosAsyncClient cosmosClient = KafkaCosmosReflectionUtils.getSinkTaskCosmosClient(sinkTask);
            CosmosContainerProperties singlePartitionContainerProperties = getSinglePartitionContainer(cosmosClient);
            CosmosAsyncContainer container = cosmosClient.getDatabase(databaseName).getContainer(singlePartitionContainerProperties.getId());

            List<SinkRecord> sinkRecordList = new ArrayList<>();
            List<TestItem> toBeCreateItems = new ArrayList<>();

            // Create test records
            for (int i = 0; i < 3; i++) {
                TestItem testItem = TestItem.createNewItem();
                toBeCreateItems.add(testItem);

                SinkRecord sinkRecord = new SinkRecord(
                    topicName,
                    1,
                    new ConnectSchema(Schema.Type.STRING),
                    testItem.getId(),
                    new ConnectSchema(Schema.Type.MAP),
                    Utils.getSimpleObjectMapper().convertValue(
                        Utils.getSimpleObjectMapper().convertValue(testItem, ObjectNode.class),
                        new TypeReference<Map<String, Object>>() {}),
                    0L);
                sinkRecordList.add(sinkRecord);
            }

            sinkTask.put(sinkRecordList);

            // Verify items were written
            List<String> writtenItemIds = new ArrayList<>();
            String query = "select * from c";
            container.queryItems(query, TestItem.class)
                .byPage()
                .flatMap(response -> {
                    writtenItemIds.addAll(
                        response.getResults().stream().map(TestItem::getId).collect(Collectors.toList()));
                    return Mono.empty();
                })
                .blockLast();

            assertThat(writtenItemIds.size()).isEqualTo(toBeCreateItems.size());
            List<String> toBeCreateItemIds = toBeCreateItems.stream().map(TestItem::getId).collect(Collectors.toList());
            assertThat(writtenItemIds.containsAll(toBeCreateItemIds)).isTrue();

        } finally {
            sinkTask.stop();
        }
    }

    @Test(groups = { "kafka", "kafka-emulator" }, timeOut = TIMEOUT)
    public void sourceTaskWithCustomAuthentication() {
        Map<String, String> sourceConfigMap = new HashMap<>();
        sourceConfigMap.put("azure.cosmos.account.endpoint", KafkaCosmosTestConfigurations.HOST);
        // Use CUSTOM auth type with our test credential provider
        sourceConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sourceConfigMap.put("credentials.provider.class", TestTokenCredential.class.getName());
        sourceConfigMap.put("azure.cosmos.source.database.name", databaseName);
        sourceConfigMap.put("azure.cosmos.source.containers.includedList", Arrays.asList(singlePartitionContainerName).toString());
        sourceConfigMap.put("azure.cosmos.source.task.id", UUID.randomUUID().toString());
        sourceConfigMap.put("azure.cosmos.source.task.feedRangeTaskUnits", "[]"); // Empty but valid JSON array

        CosmosSourceTask sourceTask = new CosmosSourceTask();

        try {
            sourceTask.start(sourceConfigMap);

            // Verify that the task started successfully with custom authentication
            // The fact that it doesn't throw an exception indicates success
            assertThat(sourceTask).isNotNull();

        } finally {
            sourceTask.stop();
        }
    }

    @Test(groups = { "unit" })
    public void sinkConnectorValidatesCustomAuthConfig() {
        CosmosSinkConnector sinkConnector = new CosmosSinkConnector();

        Map<String, String> sinkConfigMap = new HashMap<>();
        sinkConfigMap.put("azure.cosmos.account.endpoint", KafkaCosmosTestConfigurations.HOST);
        sinkConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sinkConfigMap.put("credentials.provider.class", TestTokenCredential.class.getName());
        sinkConfigMap.put("azure.cosmos.sink.database.name", "testDatabase");
        sinkConfigMap.put("azure.cosmos.sink.containers.topicMap", "topic#container");
        sinkConfigMap.put("customProperty", "customValue");

        // This should not throw any validation errors
        Config config = sinkConnector.validate(sinkConfigMap);
        boolean hasErrors = config.configValues().stream()
            .anyMatch(configValue -> !configValue.errorMessages().isEmpty());

        assertThat(hasErrors).isFalse();
    }

    @Test(groups = { "unit" })
    public void sourceConnectorValidatesCustomAuthConfig() {
        CosmosSourceConnector sourceConnector = new CosmosSourceConnector();

        Map<String, String> sourceConfigMap = new HashMap<>();
        sourceConfigMap.put("azure.cosmos.account.endpoint", KafkaCosmosTestConfigurations.HOST);
        sourceConfigMap.put("azure.cosmos.auth.type", CosmosAuthType.CUSTOM.getName());
        sourceConfigMap.put("credentials.provider.class", ConfigurableTestTokenCredential.class.getName());
        sourceConfigMap.put("azure.cosmos.source.database.name", "testDatabase");
        sourceConfigMap.put("azure.cosmos.source.containers.includedList", Arrays.asList("container1").toString());
        sourceConfigMap.put("token", "custom-token");
        sourceConfigMap.put("expiryMinutes", "45");

        // This should not throw any validation errors
        Config config = sourceConnector.validate(sourceConfigMap);
        boolean hasErrors = config.configValues().stream()
            .anyMatch(configValue -> !configValue.errorMessages().isEmpty());

        assertThat(hasErrors).isFalse();
    }
}
