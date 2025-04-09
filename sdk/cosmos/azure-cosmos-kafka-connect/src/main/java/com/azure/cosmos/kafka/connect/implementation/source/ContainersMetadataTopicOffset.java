// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation.source;

import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.FeedRange;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.azure.cosmos.implementation.guava25.base.Preconditions.checkNotNull;

/**
 * Containers metadata topic offset.
 */
public class ContainersMetadataTopicOffset {
    private static final Logger logger = LoggerFactory.getLogger(ContainersMetadataTopicOffset.class);

    public static final String CONTAINERS_RESOURCE_IDS_NAME_KEY = "containerRids";
    public static final String CONTAINER_FEED_RANGES_KEY = "feedRanges";
    public static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();

    private final List<String> containerRids;
    /**
     * The feed ranges associated with the containers.
     */
    private final List<FeedRange> feedRanges;

    /**
     * Creates a new instance of ContainersMetadataTopicOffset.
     *
     * @param containerRids The list of container resource IDs
     * @param feedRanges The list of feed ranges associated with the containers (can be null)
     */
    public ContainersMetadataTopicOffset(List<String> containerRids, List<FeedRange> feedRanges) {
        checkNotNull(containerRids, "Argument 'containerRids' can not be null");
        this.containerRids = containerRids;
        this.feedRanges = feedRanges != null ? feedRanges : Collections.emptyList();

        // Log when the new functionality is used
        if (!this.feedRanges.isEmpty()) {
            logger.info("Created ContainersMetadataTopicOffset with feedRanges: {}", this.feedRanges);
        }
    }

    public List<String> getContainerRids() {
        return containerRids;
    }

    /**
     * Gets the feed ranges associated with the containers.
     *
     * @return The list of feed ranges, or an empty list if none were specified
     */
    public List<FeedRange> getFeedRanges() {
        return feedRanges;
    }

    public static Map<String, Object> toMap(ContainersMetadataTopicOffset offset) {
        Map<String, Object> map = new HashMap<>();
        try {
            map.put(
                CONTAINERS_RESOURCE_IDS_NAME_KEY,
                OBJECT_MAPPER.writeValueAsString(offset.getContainerRids()));

            if (offset.getFeedRanges() != null && !offset.getFeedRanges().isEmpty()) {
                map.put(
                    CONTAINER_FEED_RANGES_KEY,
                    OBJECT_MAPPER.writeValueAsString(
                        offset.getFeedRanges().stream()
                            .map(FeedRange::toString)
                            .collect(Collectors.toList())));
                logger.info("Converting to map with feedRanges: {}", offset.getFeedRanges());
            }
            return map;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static ContainersMetadataTopicOffset fromMap(Map<String, Object> offsetMap) {
        if (offsetMap == null) {
            return null;
        }

        try {
            List<String> containerRids =
                OBJECT_MAPPER.readValue(
                    offsetMap.get(CONTAINERS_RESOURCE_IDS_NAME_KEY).toString(),
                    new TypeReference<List<String>>() {});

            List<FeedRange> feedRanges = null;
            if (offsetMap.containsKey(CONTAINER_FEED_RANGES_KEY)) {
                feedRanges = OBJECT_MAPPER
                    .readValue(offsetMap.get(CONTAINER_FEED_RANGES_KEY).toString(),
                        new TypeReference<List<String>>() {})
                    .stream()
                    .map(FeedRange::fromString)
                    .collect(Collectors.toList());
                logger.info("Parsed from map with feedRanges: {}", feedRanges);
            }

            return new ContainersMetadataTopicOffset(containerRids, feedRanges);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
