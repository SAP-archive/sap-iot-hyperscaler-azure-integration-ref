package com.sap.iot.azure.ref.integration.commons.context;

import com.microsoft.azure.functions.ExecutionContext;
import org.apache.logging.log4j.ThreadContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InvocationContextTest {

    private static final String INVOCATION_ID = UUID.randomUUID().toString();
    private List<LogRecord> logRecords = new LinkedList<>();

    @Mock
    ExecutionContext context;

    @BeforeClass
    public static void setupCalass() {
        // clean up before the start of the test
        InvocationContext.closeInitializationContext();
        InvocationContext.closeInvocationContext();

        InvocationContext.setupInitializationContext("test-class-init");
        assertEquals("test-class-init", InvocationContext.getContext().getFunctionName());
    }

    @Before
    public void setup() {
        logRecords.clear();
        Logger testLogger = Logger.getLogger(InvocationContextTest.class.getName());
        doReturn(testLogger).when(context).getLogger();
        when(context.getInvocationId()).thenReturn(INVOCATION_ID);
        InvocationContext.setupInvocationContext(context);

        testLogger.addHandler(new Handler() {
            @Override
            public void publish(LogRecord record) {
                logRecords.add(record);
            }

            @Override
            public void flush() {

            }

            @Override
            public void close() throws SecurityException {

            }
        });
    }

    @AfterClass
    public static void afterClass() {
        InvocationContext.closeInitializationContext();
    }

    @Test
    public void testInitialize() {
        assertEquals(INVOCATION_ID,ThreadContext.get("invocation-id"));
        InvocationContext.getLogger().log(Level.INFO,"testMessage");

        assertEquals(context, InvocationContext.getContext());
        assertEquals("[com.sap.iot.azure.ref.integration.commons.context.InvocationContextTest testInitialize] testMessage", logRecords.get(0).getMessage());
    }

    @Test
    public void testGetContext() {
        assertEquals(context, InvocationContext.getContext());
    }

    @Test
    public void testGetLogger() {
        Logger logger = InvocationContext.getLogger();
        // logger should be returned if InvocationContext is not initialized
        assertNotNull(logger);
    }

    @Test
    public void testCloseThreadContext(){
        InvocationContext.closeInvocationContext();
        assertTrue(ThreadContext.isEmpty());
    }

    @Test
    public void testContextInNewThreadRunnable() {
        // new thread started without passing context - no invocation id is captured;
        CompletableFuture.runAsync(() -> assertTrue(InvocationContext.getContext().getInvocationId().isEmpty())).join();

        CompletableFuture.runAsync(InvocationContext.withContext(() -> {
            // ensure that same invocation id available in the new thread
            assertEquals(InvocationContext.getContext().getInvocationId(), INVOCATION_ID);
        })).join();
    }

    @Test
    public void testContextInNewThreadSupplier() {
        // new thread started without passing context - no invocation id is captured;
        CompletableFuture.supplyAsync(() -> {
            // ensure that same invocation id available in the new thread
            assertTrue(InvocationContext.getContext().getInvocationId().isEmpty());
            return "empty";
        }).join();

        CompletableFuture.supplyAsync(InvocationContext.withContext((Supplier<String>) () -> {
            // ensure that same invocation id available in the new thread
            assertEquals(InvocationContext.getContext().getInvocationId(), INVOCATION_ID);
            return "empty";
        })).join();
    }

    @Test
    public void testContextInNewThreadCallable() throws ExecutionException, InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(1);

        // new thread started without passing context - no invocation id is captured;
        executorService.submit(() -> {
            // ensure that same invocation id available in the new thread
            assertTrue(InvocationContext.getContext().getInvocationId().isEmpty());
            return "empty";
        }).get();

        executorService.submit(InvocationContext.withContext((Callable<String>) () -> {
            // ensure that same invocation id available in the new thread
            assertEquals(InvocationContext.getContext().getInvocationId(), INVOCATION_ID);
            return "empty";
        })).get();
    }
}
