// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.securityinsights.models;

import com.azure.core.annotation.Fluent;
import com.azure.core.util.logging.ClientLogger;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.OffsetDateTime;
import java.util.List;

/** Base alert rule template property bag. */
@Fluent
public class AlertRuleTemplatePropertiesBase {
    @JsonIgnore private final ClientLogger logger = new ClientLogger(AlertRuleTemplatePropertiesBase.class);

    /*
     * the number of alert rules that were created by this template
     */
    @JsonProperty(value = "alertRulesCreatedByTemplateCount")
    private Integer alertRulesCreatedByTemplateCount;

    /*
     * The last time that this alert rule template has been updated.
     */
    @JsonProperty(value = "lastUpdatedDateUTC", access = JsonProperty.Access.WRITE_ONLY)
    private OffsetDateTime lastUpdatedDateUtc;

    /*
     * The time that this alert rule template has been added.
     */
    @JsonProperty(value = "createdDateUTC", access = JsonProperty.Access.WRITE_ONLY)
    private OffsetDateTime createdDateUtc;

    /*
     * The description of the alert rule template.
     */
    @JsonProperty(value = "description")
    private String description;

    /*
     * The display name for alert rule template.
     */
    @JsonProperty(value = "displayName")
    private String displayName;

    /*
     * The required data sources for this template
     */
    @JsonProperty(value = "requiredDataConnectors")
    private List<AlertRuleTemplateDataSource> requiredDataConnectors;

    /*
     * The alert rule template status.
     */
    @JsonProperty(value = "status")
    private TemplateStatus status;

    /**
     * Get the alertRulesCreatedByTemplateCount property: the number of alert rules that were created by this template.
     *
     * @return the alertRulesCreatedByTemplateCount value.
     */
    public Integer alertRulesCreatedByTemplateCount() {
        return this.alertRulesCreatedByTemplateCount;
    }

    /**
     * Set the alertRulesCreatedByTemplateCount property: the number of alert rules that were created by this template.
     *
     * @param alertRulesCreatedByTemplateCount the alertRulesCreatedByTemplateCount value to set.
     * @return the AlertRuleTemplatePropertiesBase object itself.
     */
    public AlertRuleTemplatePropertiesBase withAlertRulesCreatedByTemplateCount(
        Integer alertRulesCreatedByTemplateCount) {
        this.alertRulesCreatedByTemplateCount = alertRulesCreatedByTemplateCount;
        return this;
    }

    /**
     * Get the lastUpdatedDateUtc property: The last time that this alert rule template has been updated.
     *
     * @return the lastUpdatedDateUtc value.
     */
    public OffsetDateTime lastUpdatedDateUtc() {
        return this.lastUpdatedDateUtc;
    }

    /**
     * Get the createdDateUtc property: The time that this alert rule template has been added.
     *
     * @return the createdDateUtc value.
     */
    public OffsetDateTime createdDateUtc() {
        return this.createdDateUtc;
    }

    /**
     * Get the description property: The description of the alert rule template.
     *
     * @return the description value.
     */
    public String description() {
        return this.description;
    }

    /**
     * Set the description property: The description of the alert rule template.
     *
     * @param description the description value to set.
     * @return the AlertRuleTemplatePropertiesBase object itself.
     */
    public AlertRuleTemplatePropertiesBase withDescription(String description) {
        this.description = description;
        return this;
    }

    /**
     * Get the displayName property: The display name for alert rule template.
     *
     * @return the displayName value.
     */
    public String displayName() {
        return this.displayName;
    }

    /**
     * Set the displayName property: The display name for alert rule template.
     *
     * @param displayName the displayName value to set.
     * @return the AlertRuleTemplatePropertiesBase object itself.
     */
    public AlertRuleTemplatePropertiesBase withDisplayName(String displayName) {
        this.displayName = displayName;
        return this;
    }

    /**
     * Get the requiredDataConnectors property: The required data sources for this template.
     *
     * @return the requiredDataConnectors value.
     */
    public List<AlertRuleTemplateDataSource> requiredDataConnectors() {
        return this.requiredDataConnectors;
    }

    /**
     * Set the requiredDataConnectors property: The required data sources for this template.
     *
     * @param requiredDataConnectors the requiredDataConnectors value to set.
     * @return the AlertRuleTemplatePropertiesBase object itself.
     */
    public AlertRuleTemplatePropertiesBase withRequiredDataConnectors(
        List<AlertRuleTemplateDataSource> requiredDataConnectors) {
        this.requiredDataConnectors = requiredDataConnectors;
        return this;
    }

    /**
     * Get the status property: The alert rule template status.
     *
     * @return the status value.
     */
    public TemplateStatus status() {
        return this.status;
    }

    /**
     * Set the status property: The alert rule template status.
     *
     * @param status the status value to set.
     * @return the AlertRuleTemplatePropertiesBase object itself.
     */
    public AlertRuleTemplatePropertiesBase withStatus(TemplateStatus status) {
        this.status = status;
        return this;
    }

    /**
     * Validates the instance.
     *
     * @throws IllegalArgumentException thrown if the instance is not valid.
     */
    public void validate() {
        if (requiredDataConnectors() != null) {
            requiredDataConnectors().forEach(e -> e.validate());
        }
    }
}
