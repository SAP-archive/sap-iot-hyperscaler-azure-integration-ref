package com.sap.iot.azure.ref.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil;
import com.sap.iot.azure.ref.notification.processing.NotificationHandler;
import com.sap.iot.azure.ref.notification.processing.NotificationMessage;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil.createPartitionContext;
import static com.sap.iot.azure.ref.integration.commons.context.InvocationContextTestUtil.createSystemPropertiesMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class NotificationProcessorFunctionTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private NotificationMessage notificationMessage;

    @Mock
    private NotificationHandler notificationHandler;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    NotificationProcessorFunction notificationProcessorFunction;

    public static final String PARTITION_KEY = "782-342-6723/com.sap.iot.i4c.Assignment/384109E0F2534A6A382J110/ST_J110";

    @Before
    public void prepare() {
        notificationProcessorFunction = new NotificationProcessorFunction( notificationHandler );
        InvocationContextTestUtil.initInvocationContext();
    }

    @Test
    public void testRun() throws IOException {
        List<String> notificationMessages = createNotificationMessage("/AssignmentNotificationCreateMessage.json");
        notificationProcessorFunction.run(notificationMessages, createSystemPropertiesMap(), createPartitionContext(),
                InvocationContextTestUtil.getMockContext());
        verify(notificationHandler, times(1)).executeNotificationHandling(notificationMessages, createSystemPropertiesMap());
    }

    @Test
    public void testRunWithException() throws IOException {
        List<String> notificationMessages = createNotificationMessage("/AssignmentNotificationCreateMessage.json");
        Mockito.doThrow(new RuntimeException("test error")).when(notificationHandler).executeNotificationHandling(Mockito.any(), Mockito.any());

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("test error");
        notificationProcessorFunction.run(notificationMessages, createSystemPropertiesMap(), createPartitionContext(),
                InvocationContextTestUtil.getMockContext());
    }

    public List<String> createNotificationMessage(String sampleMessage) throws IOException {
        List<String> notificationMessages = new ArrayList<>();
        String message = IOUtils.toString(this.getClass().getResourceAsStream(sampleMessage), StandardCharsets.UTF_8);
        notificationMessage = mapper.readValue(message, NotificationMessage.class);
        notificationMessage.setPartitionKey(PARTITION_KEY);
        notificationMessages.add(String.valueOf(notificationMessage));
        return notificationMessages;
    }

}