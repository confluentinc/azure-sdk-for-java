// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.cosmos.kafka.connect.KafkaCosmosReflectionUtils;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.testng.Assert.assertThrows;

public class CosmosClientStoreCustomAuthTest {

    @Test(groups = "unit")
    public void testCreateCustomTokenCredentialWithValidClass() throws Exception {
        // Arrange
        String providerClassName = TestTokenCredential.class.getName();
        Map<String, Object> configMap = new HashMap<>();
        CosmosCustomAuthConfig customAuthConfig = new CosmosCustomAuthConfig(providerClassName, configMap);

        // Act
        TokenCredential credential = invokeCreateCustomTokenCredential(customAuthConfig);

        // Assert
        assertThat(credential).isNotNull();
        assertThat(credential).isInstanceOf(TestTokenCredential.class);
        
        // Verify the credential works
        TokenRequestContext context = new TokenRequestContext();
        AccessToken token = credential.getToken(context).block();
        assertThat(token).isNotNull();
        assertThat(token.getToken()).isEqualTo("test-token-12345");
    }

    @Test(groups = "unit")
    public void testCreateCustomTokenCredentialWithConfigurableClass() throws Exception {
        // Arrange
        String providerClassName = ConfigurableTestTokenCredential.class.getName();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("token", "custom-configured-token");
        configMap.put("expiryMinutes", "30");
        configMap.put("customProperty", "customValue");
        
        CosmosCustomAuthConfig customAuthConfig = new CosmosCustomAuthConfig(providerClassName, configMap);

        // Act
        TokenCredential credential = invokeCreateCustomTokenCredential(customAuthConfig);

        // Assert
        assertThat(credential).isNotNull();
        assertThat(credential).isInstanceOf(ConfigurableTestTokenCredential.class);
        
        ConfigurableTestTokenCredential configurableCredential = (ConfigurableTestTokenCredential) credential;
        assertThat(configurableCredential.getConfiguredToken()).isEqualTo("custom-configured-token");
        assertThat(configurableCredential.getConfiguration()).isEqualTo(configMap);
        
        // Verify the credential works
        TokenRequestContext context = new TokenRequestContext();
        AccessToken token = credential.getToken(context).block();
        assertThat(token).isNotNull();
        assertThat(token.getToken()).isEqualTo("custom-configured-token");
    }

    @Test(groups = "unit")
    public void testCreateCustomTokenCredentialWithNullClassName() {
        // Arrange
        Map<String, Object> configMap = new HashMap<>();
        CosmosCustomAuthConfig customAuthConfig = new CosmosCustomAuthConfig(null, configMap);

        // Act & Assert
        verifyExceptionThrown(customAuthConfig, IllegalArgumentException.class, "Credential provider class is required for Custom authentication");
    }

    @Test(groups = "unit")
    public void testCreateCustomTokenCredentialWithEmptyClassName() {
        // Arrange
        Map<String, Object> configMap = new HashMap<>();
        CosmosCustomAuthConfig customAuthConfig = new CosmosCustomAuthConfig("", configMap);

        // Act & Assert
        verifyExceptionThrown(customAuthConfig, IllegalArgumentException.class, "Credential provider class is required for Custom authentication");
    }

    @Test(groups = "unit")
    public void testCreateCustomTokenCredentialWithWhitespaceClassName() {
        // Arrange
        Map<String, Object> configMap = new HashMap<>();
        CosmosCustomAuthConfig customAuthConfig = new CosmosCustomAuthConfig("   ", configMap);

        // Act & Assert
        verifyExceptionThrown(customAuthConfig, IllegalArgumentException.class, "Credential provider class is required for Custom authentication");
    }

    @Test(groups = "unit")
    public void testCreateCustomTokenCredentialWithNonExistentClass() {
        // Arrange
        String nonExistentClassName = "com.nonexistent.FakeTokenCredential";
        Map<String, Object> configMap = new HashMap<>();
        CosmosCustomAuthConfig customAuthConfig = new CosmosCustomAuthConfig(nonExistentClassName, configMap);

        // Act & Assert
        verifyExceptionThrown(customAuthConfig, IllegalArgumentException.class, "startsWith:Credential provider class not found: " + nonExistentClassName);
    }

    @Test(groups = "unit")
    public void testCreateCustomTokenCredentialWithClassThatDoesNotImplementTokenCredential() {
        // Arrange
        String invalidClassName = InvalidTestTokenCredential.class.getName();
        Map<String, Object> configMap = new HashMap<>();
        CosmosCustomAuthConfig customAuthConfig = new CosmosCustomAuthConfig(invalidClassName, configMap);

        // Act & Assert
        verifyExceptionThrown(customAuthConfig, IllegalArgumentException.class, "contains:cannot be cast to TokenCredential");
    }

    @Test(groups = "unit")
    public void testCreateCustomTokenCredentialWithClassWithoutDefaultConstructor() {
        // Arrange
        String noDefaultConstructorClassName = NoDefaultConstructorTokenCredential.class.getName();
        Map<String, Object> configMap = new HashMap<>();
        CosmosCustomAuthConfig customAuthConfig = new CosmosCustomAuthConfig(noDefaultConstructorClassName, configMap);

        // Act & Assert
        verifyExceptionThrown(customAuthConfig, IllegalArgumentException.class, "startsWith:Credential provider class must have a public no-argument constructor: " + noDefaultConstructorClassName);
    }

    @Test(groups = "unit")
    public void testCreateCustomTokenCredentialWithAbstractClass() {
        // Arrange
        String abstractClassName = "com.azure.core.credential.TokenCredential"; // This is an interface, but should trigger similar error
        Map<String, Object> configMap = new HashMap<>();
        CosmosCustomAuthConfig customAuthConfig = new CosmosCustomAuthConfig(abstractClassName, configMap);

        // Act & Assert
        verifyExceptionThrown(customAuthConfig, IllegalArgumentException.class, "contains:public no-argument constructor");
    }

    /**
     * Helper method to invoke the private createCustomTokenCredential method using reflection
     */
    private TokenCredential invokeCreateCustomTokenCredential(CosmosCustomAuthConfig customAuthConfig) throws Exception {
        Method method = CosmosClientCache.class.getDeclaredMethod("createCustomTokenCredential", CosmosCustomAuthConfig.class);
        method.setAccessible(true);
        CosmosClientCache cache = CosmosClientCache.getInstance();
        return (TokenCredential) method.invoke(cache, customAuthConfig);
    }
    
    /**
     * Helper method to verify that the correct exception is thrown, handling reflection wrappers
     */
    private void verifyExceptionThrown(CosmosCustomAuthConfig customAuthConfig, Class<? extends Exception> expectedExceptionClass, String expectedMessage) {
        try {
            invokeCreateCustomTokenCredential(customAuthConfig);
            // Should not reach here
            assertThat(false).as("Expected exception was not thrown").isTrue();
        } catch (Exception e) {
            Exception actualException = e;
            if (e.getCause() instanceof Exception) {
                actualException = (Exception) e.getCause();
            }
            assertThat(actualException).isInstanceOf(expectedExceptionClass);
            if (expectedMessage != null) {
                if (expectedMessage.startsWith("startsWith:")) {
                    assertThat(actualException.getMessage()).startsWith(expectedMessage.substring(11));
                } else if (expectedMessage.startsWith("contains:")) {
                    assertThat(actualException.getMessage()).contains(expectedMessage.substring(9));
                } else {
                    assertThat(actualException.getMessage()).isEqualTo(expectedMessage);
                }
            }
        }
    }
}
