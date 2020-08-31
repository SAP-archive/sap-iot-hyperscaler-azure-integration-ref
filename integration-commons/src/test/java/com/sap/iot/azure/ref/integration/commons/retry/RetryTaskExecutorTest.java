package com.sap.iot.azure.ref.integration.commons.retry;

import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class RetryTaskExecutorTest {

    @Mock
    private Callable<CompletableFuture<Void>> mockCallable;
    private RetryTaskExecutor retryTaskExecutor;
    private static final int MAX_TRIES = 4;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setup() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Before
    public void setupTest() {
        reset(mockCallable);
        retryTaskExecutor = spy(new RetryTaskExecutor());
        // always returns 1s (without exponential backoff) for test purpose only
        doReturn(1).when(retryTaskExecutor).getNextDelay(anyInt());
    }

    @Test
    public void testSucceedWithoutRetry() throws Exception {
        when(mockCallable.call()).thenReturn(CompletableFuture.completedFuture(null));
        retryTaskExecutor.executeWithRetry(mockCallable, MAX_TRIES).get();
        Mockito.verify(mockCallable, Mockito.times(1)).call();
    }

    @Test(expected = ExecutionException.class)
    public void testFailedWithRetry() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Retry failed!"));
        when(mockCallable.call()).thenReturn(future);
        retryTaskExecutor.executeWithRetry(mockCallable, MAX_TRIES).get();
        Mockito.verify(mockCallable, Mockito.times(MAX_TRIES)).call();
    }

    @Test
    public void testSucceedWithRetry() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Retry failed!"));
        when(mockCallable.call()).thenReturn(future).thenReturn(future).thenReturn(CompletableFuture.completedFuture(null));
        retryTaskExecutor.executeWithRetry(mockCallable, MAX_TRIES).get();
        Mockito.verify(mockCallable, Mockito.times(3)).call();
    }

    @Test
    public void testWithNonTransientException() throws Exception {

        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(IoTRuntimeException.wrapNonTransient(IdentifierUtil.empty(), CommonErrorType.MAPPING_LOOKUP_ERROR, "permanent-exceptionception"));
        when(mockCallable.call()).thenReturn(future);

        try {
            // not using ExpectedException rule since the verify after exception is not executed
            retryTaskExecutor.executeWithRetry(mockCallable, MAX_TRIES).get();
        } catch(ExecutionException ex) {
              assertTrue(ex.getMessage().contains("permanent-exception"));
        }

        // the given task should be executed only once - since it throws an non-transient exception from the beginning
        verify(mockCallable, times(1)).call();
    }

    @Test
    public void testWithTransientException() throws Exception {

        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(IoTRuntimeException.wrapTransient(IdentifierUtil.empty(), CommonErrorType.MAPPING_LOOKUP_ERROR, "transient-exception"));
        when(mockCallable.call()).thenReturn(future);

        try {
            retryTaskExecutor.executeWithRetry(mockCallable, MAX_TRIES).get();
        } catch (ExecutionException ex) {
            assertTrue(ex.getMessage().contains("transient-exception"));
        }

        // given task to be executed for all max_tries, since the task throws a transient exception every single execution
        verify(mockCallable, times(MAX_TRIES)).call();
    }

    @Test
    public void testWithTransientAndThenPermanentException() throws Exception {

        CompletableFuture<Void> future1 = new CompletableFuture<>();
        future1.completeExceptionally(IoTRuntimeException.wrapTransient(IdentifierUtil.empty(), CommonErrorType.MAPPING_LOOKUP_ERROR, "transient-exception"));

        CompletableFuture<Void> future2 = new CompletableFuture<>();
        future2.completeExceptionally(IoTRuntimeException.wrapNonTransient(IdentifierUtil.empty(), CommonErrorType.MAPPING_LOOKUP_ERROR, "permanent-exception"));

        when(mockCallable.call()).thenReturn(future1).thenReturn(future2);

        try {
            retryTaskExecutor.executeWithRetry(mockCallable, MAX_TRIES).get();
        } catch (ExecutionException ex) {
            assertTrue(ex.getMessage().contains("permanent-exception"));
        }

        verify(mockCallable, times(2)).call();
    }
}