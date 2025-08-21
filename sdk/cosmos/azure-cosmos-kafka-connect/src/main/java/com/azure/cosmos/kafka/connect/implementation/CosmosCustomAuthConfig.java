package com.azure.cosmos.kafka.connect.implementation;

import java.util.Map;

public class CosmosCustomAuthConfig implements CosmosAuthConfig {
    private final String credentialProviderClass;
    private final Map<String, Object> configMap;

    public CosmosCustomAuthConfig(String credentialProviderClass, Map<String, Object> configMap) {
        this.credentialProviderClass = credentialProviderClass;
        this.configMap = configMap;
    }

    public String getCredentialProviderClass() {
        return credentialProviderClass;
    }

    public Map<String, Object> getConfigMap() {
        return configMap;
    }
}
