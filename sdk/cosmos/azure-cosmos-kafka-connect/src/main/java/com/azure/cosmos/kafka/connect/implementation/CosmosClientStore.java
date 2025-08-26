// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.implementation.CosmosClientMetadataCachesSnapshot;
import com.azure.cosmos.implementation.ImplementationBridgeHelpers;
import com.azure.cosmos.implementation.apachecommons.lang.StringUtils;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class CosmosClientStore {
    private static final Map<CosmosAzureEnvironment, String> ACTIVE_DIRECTORY_ENDPOINT_MAP;
    static {
        // for now we maintain a static list within the SDK these values do not change very frequently
        ACTIVE_DIRECTORY_ENDPOINT_MAP = new HashMap<>();
        ACTIVE_DIRECTORY_ENDPOINT_MAP.put(CosmosAzureEnvironment.AZURE, "https://login.microsoftonline.com/");
        ACTIVE_DIRECTORY_ENDPOINT_MAP.put(CosmosAzureEnvironment.AZURE_CHINA, "https://login.chinacloudapi.cn/");
        ACTIVE_DIRECTORY_ENDPOINT_MAP.put(CosmosAzureEnvironment.AZURE_US_GOVERNMENT, "https://login.microsoftonline.us/");
        ACTIVE_DIRECTORY_ENDPOINT_MAP.put(CosmosAzureEnvironment.AZURE_GERMANY, "https://login.microsoftonline.de/");
    }

    public static CosmosAsyncClient getCosmosClient(
        CosmosAccountConfig accountConfig,
        String sourceName) {

        return getCosmosClient(accountConfig, sourceName, null);
    }

    public static CosmosAsyncClient getCosmosClient(
        CosmosAccountConfig accountConfig,
        String sourceName,
        CosmosClientMetadataCachesSnapshot snapshot) {
        if (accountConfig == null) {
            return null;
        }

        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
            .endpoint(accountConfig.getEndpoint())
            .preferredRegions(accountConfig.getPreferredRegionsList())
            .throttlingRetryOptions(
                new ThrottlingRetryOptions()
                    .setMaxRetryAttemptsOnThrottledRequests(Integer.MAX_VALUE)
                    .setMaxRetryWaitTime(Duration.ofSeconds((Integer.MAX_VALUE / 1000) - 1)))
            .userAgentSuffix(getUserAgentSuffix(accountConfig, sourceName));

        if (accountConfig.isUseGatewayMode()) {
            cosmosClientBuilder.gatewayMode(new GatewayConnectionConfig().setMaxConnectionPoolSize(10000));
        }

        if (accountConfig.getCosmosAuthConfig() instanceof CosmosMasterKeyAuthConfig) {
            cosmosClientBuilder.key(((CosmosMasterKeyAuthConfig) accountConfig.getCosmosAuthConfig()).getMasterKey());
        } else if (accountConfig.getCosmosAuthConfig() instanceof CosmosAadAuthConfig) {

            CosmosAadAuthConfig aadAuthConfig = (CosmosAadAuthConfig) accountConfig.getCosmosAuthConfig();
            ClientSecretCredential tokenCredential = new ClientSecretCredentialBuilder()
                .authorityHost(ACTIVE_DIRECTORY_ENDPOINT_MAP.get(aadAuthConfig.getAzureEnvironment()).replaceAll("/$", "") + "/")
                .tenantId(aadAuthConfig.getTenantId())
                .clientId(aadAuthConfig.getClientId())
                .clientSecret(aadAuthConfig.getClientSecret())
                .build();
            cosmosClientBuilder.credential(tokenCredential);
        } else if (accountConfig.getCosmosAuthConfig() instanceof CosmosCustomAuthConfig) {
            CosmosCustomAuthConfig customAuthConfig = (CosmosCustomAuthConfig) accountConfig.getCosmosAuthConfig();
            TokenCredential tokenCredential = createCustomTokenCredential(customAuthConfig);
            cosmosClientBuilder.credential(tokenCredential);
        } else {
            throw new IllegalArgumentException("Authorization type " + accountConfig.getCosmosAuthConfig().getClass() + " is not supported");
        }

        if (snapshot != null) {
            ImplementationBridgeHelpers.CosmosClientBuilderHelper
                .getCosmosClientBuilderAccessor()
                .setCosmosClientMetadataCachesSnapshot(cosmosClientBuilder, snapshot);
        }

        return cosmosClientBuilder.buildAsyncClient();
    }

    private static String getUserAgentSuffix(CosmosAccountConfig accountConfig, String sourceName) {
        String userAgentSuffix = KafkaCosmosConstants.USER_AGENT_SUFFIX;
        if (StringUtils.isNotEmpty(sourceName)) {
            userAgentSuffix += "|" + sourceName;
        }

        if (StringUtils.isNotEmpty(accountConfig.getApplicationName())) {
            userAgentSuffix += "|" + accountConfig.getApplicationName();
        }

        return userAgentSuffix;
    }
    /**
     * Dynamically loads and instantiates a custom credential provider for Cosmos DB authentication.
     * <p>
     * The credential provider class specified in {@code customAuthConfig} must:
     * <ul>
     *   <li>Implement the {@link com.azure.core.credential.TokenCredential} interface.</li>
     *   <li>Optionally implement {@link org.apache.kafka.common.Configurable} to receive configuration via {@code configure(Map<String, ?>)}.</li>
     * </ul>
     * The class is loaded by name using {@link Class#forName(String)}, instantiated using its public no-argument constructor,
     * and, if it implements {@code Configurable}, configured with the config map from {@code customAuthConfig}.
     * <p>
     * @param customAuthConfig The custom authentication configuration containing the credential provider class name and configuration map.
     * @return An instance of {@link TokenCredential} for authentication.
     * @throws IllegalArgumentException If the class name is missing, the class does not implement {@code TokenCredential},
     *                                 the class cannot be found, or instantiation/configuration fails.
     */
    private static TokenCredential createCustomTokenCredential(CosmosCustomAuthConfig customAuthConfig) {
        String providerClassName = customAuthConfig.getCredentialProviderClass();

        if (providerClassName == null || providerClassName.trim().isEmpty()) {
            throw new IllegalArgumentException("Credential provider class is required for Custom authentication");
        }

        try {
            Class<?> providerClass = Class.forName(providerClassName);

            if (!TokenCredential.class.isAssignableFrom(providerClass)) {
                throw new IllegalArgumentException("Provider class must implement TokenCredential interface: " + providerClassName);
            }

            Object provider = providerClass.getConstructor().newInstance();

            if (provider instanceof org.apache.kafka.common.Configurable) {
                ((org.apache.kafka.common.Configurable) provider).configure(customAuthConfig.getConfigMap());
            }

            return (TokenCredential) provider;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Credential provider class not found: " + providerClassName, e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Credential provider class must have a public no-argument constructor: " + providerClassName, e);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create credential provider: " + providerClassName + ". " + e.getMessage(), e);
        }
    }
}
