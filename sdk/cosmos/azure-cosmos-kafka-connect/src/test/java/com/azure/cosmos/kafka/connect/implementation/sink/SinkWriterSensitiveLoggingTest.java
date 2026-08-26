// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.implementation.sink;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.fail;

/**
 * Verifies the sink write path does not log a raw {@link CosmosException}. Its {@code toString()} renders
 * {@code resourceAddress} (dbs/{db}/colls/{coll}/docs/{id}), and {@code {id}} is a record-derived document id
 * (customer data) for id-targeted writes. This covers the shared mechanism behind the CosmosWriterBase (ERROR)
 * / CosmosBulkWriter / CosmosPointWriter (WARN, tolerate-all) findings; all three route through
 * {@code KafkaCosmosExceptionsHelper.getSafeExceptionDiagnostics}.
 */
public class SinkWriterSensitiveLoggingTest {

    @Test(groups = { "unit" })
    public void writeFailureDoesNotLogRawCosmosException() {
        // Synthetic stand-in for a record-derived document id embedded in the SDK exception's resourceAddress.
        String canaryResourceAddress = "dbs/db/colls/coll/docs/secret-record-id-canary-9f3a2b";
        CosmosException cosmosException = new CanaryCosmosException(canaryResourceAddress, 500);

        CosmosWriterBase writer = new CosmosWriterBase(null) {
            @Override
            void writeCore(CosmosAsyncContainer container, List<SinkOperation> sinkOperations) {
                throw cosmosException;
            }
        };

        CosmosAsyncContainer container = Mockito.mock(CosmosAsyncContainer.class);
        Mockito.when(container.getId()).thenReturn("test-container");
        SinkRecord sinkRecord = new SinkRecord("test-topic", 0, null, "k", null, "v", 42);

        Logger coreLogger = (Logger) LogManager.getLogger(CosmosWriterBase.class);
        CapturingAppender appender = new CapturingAppender();
        appender.start();
        Level previousLevel = coreLogger.getLevel();
        coreLogger.addAppender(appender);
        Configurator.setLevel(CosmosWriterBase.class.getName(), Level.ERROR);

        try {
            writer.write(container, Collections.singletonList(sinkRecord));
            fail("write() should rethrow as CosmosWriteException");
        } catch (CosmosWriteException expected) {
            // expected: write() logs then rethrows
        } finally {
            coreLogger.removeAppender(appender);
            appender.stop();
            Configurator.setLevel(CosmosWriterBase.class.getName(), previousLevel);
        }

        LogEvent errorEvent = null;
        for (LogEvent event : appender.getEvents()) {
            if (event.getLevel() == Level.ERROR
                && event.getMessage().getFormattedMessage().startsWith("Write failed.")) {
                errorEvent = event;
                break;
            }
        }

        assertThat(errorEvent).as("expected a 'Write failed.' ERROR log").isNotNull();
        // Structural lever: the raw exception must not be attached to the log event. A trailing throwable is
        // rendered by the layout as a stack trace whose header is exception.toString() (carrying
        // resourceAddress); dropping the throwable is what closes the leak.
        assertThat(errorEvent.getThrown()).as("raw exception must not be logged").isNull();
        // A safe correlator is logged instead of the raw exception.
        assertThat(errorEvent.getMessage().getFormattedMessage()).contains("statusCode");
        // Belt-and-suspenders: the record-derived resourceAddress appears nowhere in what would be emitted.
        String rendered = errorEvent.getMessage().getFormattedMessage()
            + (errorEvent.getThrown() == null ? "" : errorEvent.getThrown().toString());
        assertThat(rendered).doesNotContain(canaryResourceAddress);
    }

    /** A CosmosException whose real toString() renders {@code resourceAddress}, exactly as the SDK does. */
    private static final class CanaryCosmosException extends CosmosException {
        CanaryCosmosException(String resourceAddress, int statusCode) {
            super(resourceAddress, statusCode, null, null);
        }
    }

    private static final class CapturingAppender extends AbstractAppender {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        CapturingAppender() {
            super("capture", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }

        List<LogEvent> getEvents() {
            return events;
        }
    }
}
