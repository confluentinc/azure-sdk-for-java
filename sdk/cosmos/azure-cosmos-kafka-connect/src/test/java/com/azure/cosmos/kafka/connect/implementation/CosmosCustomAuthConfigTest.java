// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class CosmosCustomAuthConfigTest {

    @Test(groups = "unit")
    public void testCosmosCustomAuthConfigCreation() {
        // Arrange
        String providerClassName = "com.example.TestTokenCredential";
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("property1", "value1");
        configMap.put("property2", 42);

        // Act
        CosmosCustomAuthConfig authConfig = new CosmosCustomAuthConfig(providerClassName, configMap);

        // Assert
        assertThat(authConfig.getCredentialProviderClass()).isEqualTo(providerClassName);
        assertThat(authConfig.getConfigMap()).isEqualTo(configMap);
        assertThat(authConfig.getConfigMap().get("property1")).isEqualTo("value1");
        assertThat(authConfig.getConfigMap().get("property2")).isEqualTo(42);
    }

    @Test(groups = "unit")
    public void testCosmosCustomAuthConfigWithNullClassName() {
        // Arrange
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("property1", "value1");

        // Act
        CosmosCustomAuthConfig authConfig = new CosmosCustomAuthConfig(null, configMap);

        // Assert
        assertThat(authConfig.getCredentialProviderClass()).isNull();
        assertThat(authConfig.getConfigMap()).isEqualTo(configMap);
    }

    @Test(groups = "unit")
    public void testCosmosCustomAuthConfigWithEmptyClassName() {
        // Arrange
        String emptyClassName = "";
        Map<String, Object> configMap = new HashMap<>();

        // Act
        CosmosCustomAuthConfig authConfig = new CosmosCustomAuthConfig(emptyClassName, configMap);

        // Assert
        assertThat(authConfig.getCredentialProviderClass()).isEqualTo(emptyClassName);
        assertThat(authConfig.getConfigMap()).isEqualTo(configMap);
    }

    @Test(groups = "unit")
    public void testCosmosCustomAuthConfigWithNullConfigMap() {
        // Arrange
        String providerClassName = "com.example.TestTokenCredential";

        // Act
        CosmosCustomAuthConfig authConfig = new CosmosCustomAuthConfig(providerClassName, null);

        // Assert
        assertThat(authConfig.getCredentialProviderClass()).isEqualTo(providerClassName);
        assertThat(authConfig.getConfigMap()).isNull();
    }

    @Test(groups = "unit")
    public void testCosmosCustomAuthConfigWithEmptyConfigMap() {
        // Arrange
        String providerClassName = "com.example.TestTokenCredential";
        Map<String, Object> emptyConfigMap = new HashMap<>();

        // Act
        CosmosCustomAuthConfig authConfig = new CosmosCustomAuthConfig(providerClassName, emptyConfigMap);

        // Assert
        assertThat(authConfig.getCredentialProviderClass()).isEqualTo(providerClassName);
        assertThat(authConfig.getConfigMap()).isEqualTo(emptyConfigMap);
        assertThat(authConfig.getConfigMap().isEmpty()).isTrue();
    }

    @Test(groups = "unit")
    public void testCosmosCustomAuthConfigImplementsCosmosAuthConfig() {
        // Arrange
        String providerClassName = "com.example.TestTokenCredential";
        Map<String, Object> configMap = new HashMap<>();

        // Act
        CosmosCustomAuthConfig authConfig = new CosmosCustomAuthConfig(providerClassName, configMap);

        // Assert
        assertThat(authConfig).isInstanceOf(CosmosAuthConfig.class);
    }
}
