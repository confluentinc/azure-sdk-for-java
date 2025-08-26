package com.azure.cosmos.kafka.connect.implementation;

import java.util.Map;

/**
 * Configuration class for custom authentication with Azure Cosmos DB in Kafka Connect.
 * <p>
 * This class allows users to specify a custom {@link com.azure.core.credential.TokenCredential}
 * implementation for authenticating with Azure Cosmos DB. The custom credential provider class
 * must meet the following requirements:
 * <ul>
 *   <li>Implement the {@link com.azure.core.credential.TokenCredential} interface</li>
 *   <li>Have a public no-argument constructor</li>
 *   <li>Optionally implement {@link org.apache.kafka.common.Configurable} to receive configuration parameters</li>
 * </ul>
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 * <pre>{@code
 * Map<String, Object> customConfig = new HashMap<>();
 * customConfig.put("customProperty", "value");
 *
 * CosmosCustomAuthConfig authConfig = new CosmosCustomAuthConfig(
 *     "com.example.MyCustomTokenCredential",
 *     customConfig
 * );
 * }</pre>
 *
 * <p>
 * If the credential provider class implements {@link org.apache.kafka.common.Configurable},
 * the {@code configMap} will be passed to the {@code configure(Map<String, ?> configs)} method
 * during initialization, allowing the credential provider to receive custom configuration parameters.
 * </p>
 *
 * @see com.azure.core.credential.TokenCredential
 * @see org.apache.kafka.common.Configurable
 */
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
