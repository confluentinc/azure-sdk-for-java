// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.securityinsights.models;

import com.azure.core.annotation.Fluent;
import com.azure.core.util.logging.ClientLogger;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Customs permissions required for the connector. */
@Fluent
public class CustomsPermission {
    @JsonIgnore private final ClientLogger logger = new ClientLogger(CustomsPermission.class);

    /*
     * Customs permissions name
     */
    @JsonProperty(value = "name")
    private String name;

    /*
     * Customs permissions description
     */
    @JsonProperty(value = "description")
    private String description;

    /**
     * Get the name property: Customs permissions name.
     *
     * @return the name value.
     */
    public String name() {
        return this.name;
    }

    /**
     * Set the name property: Customs permissions name.
     *
     * @param name the name value to set.
     * @return the CustomsPermission object itself.
     */
    public CustomsPermission withName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Get the description property: Customs permissions description.
     *
     * @return the description value.
     */
    public String description() {
        return this.description;
    }

    /**
     * Set the description property: Customs permissions description.
     *
     * @param description the description value to set.
     * @return the CustomsPermission object itself.
     */
    public CustomsPermission withDescription(String description) {
        this.description = description;
        return this;
    }

    /**
     * Validates the instance.
     *
     * @throws IllegalArgumentException thrown if the instance is not valid.
     */
    public void validate() {
    }
}
