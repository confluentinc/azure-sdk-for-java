// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

import com.azure.core.credential.TokenCredential;
import com.azure.cosmos.CosmosAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.implementation.CosmosClientMetadataCachesSnapshot;
import com.azure.cosmos.implementation.ImplementationBridgeHelpers;
import com.azure.cosmos.implementation.apachecommons.lang.StringUtils;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import java.time.Duration;
import org.apache.kafka.common.Configurable;

public class CosmosClientStore {
    private static final Logger logger = LoggerFactory.getLogger(CosmosClientStore.class);
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
                .authorityHost(aadAuthConfig.getAuthEndpoint())
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
            // Use current thread ClassLoader like S3 connector does
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            Class<?> providerClass = Class.forName(providerClassName, true, contextClassLoader);

            // Add detailed logging for ClassLoader debugging
            ClassLoader tokenCredentialClassLoader = TokenCredential.class.getClassLoader();
            ClassLoader providerClassLoader = providerClass.getClassLoader();

            logger.info("=== ClassLoader Debug Info ===");
            logger.info("Provider class: {}", providerClassName);
            logger.info("Context ClassLoader: {}", contextClassLoader);
            logger.info("TokenCredential ClassLoader: {}", tokenCredentialClassLoader);
            logger.info("Provider ClassLoader: {}", providerClassLoader);
            logger.info("TokenCredential class: {}", TokenCredential.class);
            logger.info("Provider class: {}", providerClass);

            // Check interface compatibility before instantiation like S3 connector
            boolean isAssignable = TokenCredential.class.isAssignableFrom(providerClass);
            logger.info("isAssignableFrom result: {}", isAssignable);

            if (!isAssignable) {
                // Additional debug info for the failure case
                logger.error("=== Interface Check Failed ===");
                logger.error("TokenCredential interfaces: ");
                for (Class<?> iface : TokenCredential.class.getInterfaces()) {
                    logger.error("  - {} (ClassLoader: {})", iface, iface.getClassLoader());
                }
                logger.error("Provider interfaces: ");
                for (Class<?> iface : providerClass.getInterfaces()) {
                    logger.error("  - {} (ClassLoader: {})", iface, iface.getClassLoader());
                }

                throw new IllegalArgumentException("Credential provider class must implement TokenCredential interface: " + providerClassName +
                    ". TokenCredential ClassLoader: " + tokenCredentialClassLoader +
                    ", Provider ClassLoader: " + providerClassLoader);
            }

            @SuppressWarnings("unchecked")
            Class<TokenCredential> tokenCredentialClass = (Class<TokenCredential>) providerClass;
            TokenCredential provider = tokenCredentialClass.getConstructor().newInstance();

            logger.info("Provider instantiated successfully: {}", provider.getClass());

            if (provider instanceof Configurable) {
                ((Configurable) provider).configure(customAuthConfig.getConfigMap());
            }

            return provider;
        } catch (ClassNotFoundException e) {
            logger.error("Class not found: {}", providerClassName, e);
            throw new IllegalArgumentException("Credential provider class not found: " + providerClassName, e);
        } catch (NoSuchMethodException e) {
            logger.error("No public no-arg constructor: {}", providerClassName, e);
            throw new IllegalArgumentException("Credential provider class must have a public no-argument constructor: " + providerClassName, e);
        } catch (ClassCastException e) {
            logger.error("ClassCastException: Cast failed even after isAssignableFrom check passed! This confirms ClassLoader isolation issue", e);
            throw new IllegalArgumentException("ClassLoader isolation prevents casting to TokenCredential: " + providerClassName + ". " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected exception creating credential provider {}: {}", providerClassName, e.getMessage(), e);
            throw new IllegalArgumentException("Failed to create credential provider: " + providerClassName + ". " + e.getMessage(), e);
        }
    }
}
