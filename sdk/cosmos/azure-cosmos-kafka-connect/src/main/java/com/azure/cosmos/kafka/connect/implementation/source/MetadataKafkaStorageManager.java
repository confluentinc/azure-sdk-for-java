// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation.source;

import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.FeedRange;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import reactor.core.publisher.Mono;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Map;
import java.util.HashMap;

import static com.azure.cosmos.kafka.connect.implementation.source.ContainersMetadataTopicOffset.CONTAINERS_RESOURCE_IDS_NAME_KEY;
import static com.azure.cosmos.kafka.connect.implementation.source.FeedRangesMetadataTopicOffset.CONTAINER_FEED_RANGES_KEY;

public class MetadataKafkaStorageManager implements IMetadataReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataKafkaStorageManager.class);
    private final OffsetStorageReader offsetStorageReader;

    public MetadataKafkaStorageManager(OffsetStorageReader offsetStorageReader) {
        this.offsetStorageReader = offsetStorageReader;
    }

    public Mono<Utils.ValueHolder<FeedRangesMetadataTopicOffset>> getFeedRangesMetadataOffset(
        String databaseName,
        String containerRid,
        String connectorName) {
        Map<String, Object> topicOffsetMap =
            this.offsetStorageReader
                .offset(
                    FeedRangesMetadataTopicPartition.toMap(
                            new FeedRangesMetadataTopicPartition(databaseName, containerRid, connectorName)));

        if (topicOffsetMap == null) {
            return Mono.just(new Utils.ValueHolder<>(null));
        }

        try {
            // The data is stored in a Struct with our unified schema
            Object value = topicOffsetMap.get("value");
            if (value instanceof Struct) {
                Struct unifiedStruct = (Struct) value;
                String feedRangesJson = unifiedStruct.getString(CONTAINER_FEED_RANGES_KEY);

                if (feedRangesJson != null) {
                    Map<String, Object> feedRangesMap = new HashMap<>();
                    feedRangesMap.put(CONTAINER_FEED_RANGES_KEY, feedRangesJson);
                    return Mono.just(new Utils.ValueHolder<>(FeedRangesMetadataTopicOffset.fromMap(feedRangesMap)));
                }
            }
            LOGGER.warn("No feed ranges found in unified schema for database: {}, containerRid: {}", databaseName, containerRid);
            return Mono.just(new Utils.ValueHolder<>(null));
        } catch (Exception e) {
            LOGGER.error("Error processing feed ranges metadata from unified schema", e);
            return Mono.error(new ConnectException("Failed to process feed ranges metadata", e));
        }
    }

    public Mono<Utils.ValueHolder<ContainersMetadataTopicOffset>> getContainersMetadataOffset(
        String databaseName,
        String connectorName) {
        Map<String, Object> topicOffsetMap =
            this.offsetStorageReader
                .offset(
                    ContainersMetadataTopicPartition.toMap(
                        new ContainersMetadataTopicPartition(databaseName, connectorName)));

        if (topicOffsetMap == null) {
            return Mono.just(new Utils.ValueHolder<>(null));
        }

        try {
            // The data is stored in a Struct with our unified schema
            Object value = topicOffsetMap.get("value");
            if (value instanceof Struct) {
                Struct unifiedStruct = (Struct) value;
                String containerRidsJson = unifiedStruct.getString(CONTAINERS_RESOURCE_IDS_NAME_KEY);

                if (containerRidsJson != null) {
                    Map<String, Object> containerRidsMap = new HashMap<>();
                    containerRidsMap.put(CONTAINERS_RESOURCE_IDS_NAME_KEY, containerRidsJson);
                    return Mono.just(new Utils.ValueHolder<>(ContainersMetadataTopicOffset.fromMap(containerRidsMap)));
                }
            }
            LOGGER.warn("No container RIDs found in unified schema for database: {}", databaseName);
            return Mono.just(new Utils.ValueHolder<>(null));
        } catch (Exception e) {
            LOGGER.error("Error processing containers metadata from unified schema", e);
            return Mono.error(new ConnectException("Failed to process containers metadata", e));
        }
    }

    public FeedRangeContinuationTopicOffset getFeedRangeContinuationOffset(
        String databaseName,
        String collectionRid,
        FeedRange feedRange) {

        Map<String, Object> topicOffsetMap =
            this.offsetStorageReader
                .offset(
                    FeedRangeContinuationTopicPartition.toMap(
                        new FeedRangeContinuationTopicPartition(databaseName, collectionRid, feedRange)));

        return FeedRangeContinuationTopicOffset.fromMap(topicOffsetMap);
    }

    public OffsetStorageReader getOffsetStorageReader() {
        return this.offsetStorageReader;
    }
}
