package io.floci.az.services.servicebus;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPStandardMessage;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ServiceBusPeekPluginTest {
    private static final String PLUGIN = "io.floci.az.artemis.ServiceBusManagementPlugin";
    private static final SimpleString PRIOR_DELIVERY_COUNT = SimpleString.of("floci_prior_delivery_count");

    @Test
    void peekCountsTheDeliveriesOfADeadLetterBeforeItWasDeadLettered() throws Exception {
        AMQPMessage deadLetter = storedMessage();
        MessageReference reference = mock(MessageReference.class);
        when(reference.getDeliveryCount()).thenReturn(1);

        try (URLClassLoader plugin = jarLoader(ServiceBusNamespaceManager.ARTEMIS_EXTENSION_RESOURCE)) {
            Method peekHeader = Class.forName(PLUGIN, true, plugin)
                    .getDeclaredMethod("peekHeader", AMQPMessage.class, MessageReference.class);
            peekHeader.setAccessible(true);
            assertEquals(UnsignedInteger.valueOf(1),
                    ((Header) peekHeader.invoke(null, deadLetter, reference)).getDeliveryCount());

            TypedProperties extraProperties = deadLetter.createExtraProperties();
            extraProperties.putIntProperty(PRIOR_DELIVERY_COUNT, 10);
            assertEquals(UnsignedInteger.valueOf(11),
                    ((Header) peekHeader.invoke(null, deadLetter, reference)).getDeliveryCount());
        }
    }

    /**
     * The AMQP layer keeps the count and this plugin reads it, so its name is spelled in two jars. A
     * spelling that drifts breaks no build: a dead letter's delivery count would read zero again.
     */
    @Test
    void theAmqpLayerAndThePluginNameThePriorDeliveryCountAlike() throws Exception {
        try (URLClassLoader patch = jarLoader(ArtemisPatchVersions.ARTEMIS_AMQP_PATCH_RESOURCE);
             URLClassLoader plugin = jarLoader(ServiceBusNamespaceManager.ARTEMIS_EXTENSION_RESOURCE)) {
            assertEquals(PRIOR_DELIVERY_COUNT, constant(patch,
                    "org.apache.activemq.artemis.protocol.amqp.proton.ServiceBusMessageMetadataSupport",
                    "PRIOR_DELIVERY_COUNT"));
            assertEquals(PRIOR_DELIVERY_COUNT, constant(plugin, PLUGIN, "PRIOR_DELIVERY_COUNT"));
        }
    }

    private static AMQPMessage storedMessage() {
        Message original = Proton.message();
        original.setBody(new AmqpValue("payload"));
        byte[] encoded = new byte[1024];
        int size = original.encode(encoded, 0, encoded.length);
        return new AMQPStandardMessage(0, Arrays.copyOf(encoded, size), null);
    }

    private URLClassLoader jarLoader(String resource) {
        URL jar = getClass().getResource(resource);
        assertNotNull(jar, "Jar not found: " + resource + "; the build writes it, so run the build");
        return new URLClassLoader(new URL[]{jar}, getClass().getClassLoader());
    }

    private static Object constant(ClassLoader loader, String className, String fieldName) throws Exception {
        Field field = Class.forName(className, true, loader).getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(null);
    }
}
