// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation;

/**
 * Mock class that does NOT implement TokenCredential for testing error scenarios
 */
public class InvalidTestTokenCredential {
    
    public InvalidTestTokenCredential() {
        // Default constructor
    }
    
    public String getInvalidMethod() {
        return "This class does not implement TokenCredential";
    }
}
