// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.securityinsights.implementation;

import com.azure.core.http.rest.Response;
import com.azure.core.http.rest.SimpleResponse;
import com.azure.core.util.Context;
import com.azure.core.util.logging.ClientLogger;
import com.azure.resourcemanager.securityinsights.fluent.EntitiesGetTimelinesClient;
import com.azure.resourcemanager.securityinsights.fluent.models.EntityTimelineResponseInner;
import com.azure.resourcemanager.securityinsights.models.EntitiesGetTimelines;
import com.azure.resourcemanager.securityinsights.models.EntityTimelineParameters;
import com.azure.resourcemanager.securityinsights.models.EntityTimelineResponse;
import com.fasterxml.jackson.annotation.JsonIgnore;

public final class EntitiesGetTimelinesImpl implements EntitiesGetTimelines {
    @JsonIgnore private final ClientLogger logger = new ClientLogger(EntitiesGetTimelinesImpl.class);

    private final EntitiesGetTimelinesClient innerClient;

    private final com.azure.resourcemanager.securityinsights.SecurityInsightsManager serviceManager;

    public EntitiesGetTimelinesImpl(
        EntitiesGetTimelinesClient innerClient,
        com.azure.resourcemanager.securityinsights.SecurityInsightsManager serviceManager) {
        this.innerClient = innerClient;
        this.serviceManager = serviceManager;
    }

    public EntityTimelineResponse list(
        String resourceGroupName, String workspaceName, String entityId, EntityTimelineParameters parameters) {
        EntityTimelineResponseInner inner =
            this.serviceClient().list(resourceGroupName, workspaceName, entityId, parameters);
        if (inner != null) {
            return new EntityTimelineResponseImpl(inner, this.manager());
        } else {
            return null;
        }
    }

    public Response<EntityTimelineResponse> listWithResponse(
        String resourceGroupName,
        String workspaceName,
        String entityId,
        EntityTimelineParameters parameters,
        Context context) {
        Response<EntityTimelineResponseInner> inner =
            this.serviceClient().listWithResponse(resourceGroupName, workspaceName, entityId, parameters, context);
        if (inner != null) {
            return new SimpleResponse<>(
                inner.getRequest(),
                inner.getStatusCode(),
                inner.getHeaders(),
                new EntityTimelineResponseImpl(inner.getValue(), this.manager()));
        } else {
            return null;
        }
    }

    private EntitiesGetTimelinesClient serviceClient() {
        return this.innerClient;
    }

    private com.azure.resourcemanager.securityinsights.SecurityInsightsManager manager() {
        return this.serviceManager;
    }
}
