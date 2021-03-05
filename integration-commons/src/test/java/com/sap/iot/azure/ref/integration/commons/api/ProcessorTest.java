package com.sap.iot.azure.ref.integration.commons.api;

import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.integration.commons.exception.CommonErrorType;
import com.sap.iot.azure.ref.integration.commons.exception.IdentifierUtil;
import com.sap.iot.azure.ref.integration.commons.exception.base.IoTRuntimeException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.logging.Level;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class ProcessorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void setupClass() {
        InvocationContextTestUtil.initInvocationContext();
    }

    @Before
    public void setup() {
        reset(InvocationContextTestUtil.LOGGER);
    }

    @Test
    public void testNonTransientExceptionHandling() {
        Processor processor = new ProcessorImpl(false);
        processor.apply(new Object());
        verify(InvocationContextTestUtil.LOGGER, times(1)).log(eq(Level.SEVERE), anyString(), any(IoTRuntimeException.class));
    }

    @Test
    public void testTransientExceptionHandling() {
        expectedException.expect(IoTRuntimeException.class);
        Processor processor = new ProcessorImpl(true);
        processor.apply(new Object());

        verify(InvocationContextTestUtil.LOGGER, times(1)).warning(anyString());
    }

    class ProcessorImpl implements Processor {

        private boolean isTransient;

        ProcessorImpl(boolean isTransient) {
            this.isTransient = isTransient;
        }

        @Override
        public Object process(Object o) throws IoTRuntimeException {
            if (isTransient) {
                throw IoTRuntimeException.wrapTransient(IdentifierUtil.empty(), CommonErrorType.MAPPING_LOOKUP_ERROR, "error", new RuntimeException("error"));
            } else {
                throw IoTRuntimeException.wrapNonTransient(IdentifierUtil.empty(), CommonErrorType.MAPPING_LOOKUP_ERROR, "error", new RuntimeException("error"));
            }
        }
    }
}