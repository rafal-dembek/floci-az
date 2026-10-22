package org.apache.activemq.artemis.protocol.amqp.proton;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPStandardMessage;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.URLClassLoader;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ServiceBusDeadLetterSupportTest {
    private static final String ENTITY = "orders";
    private static final String PEEK_LOCK_QUEUE = "floci-az:servicebus-peeklock:60";
    private static final int MAX_DELIVERY_COUNT = 10;
    private static final SimpleString PRIOR_DELIVERY_COUNT = SimpleString.of("floci_prior_delivery_count");
    private static final long DEAD_LETTER_ID = 2;

    @Test
    void convertsUnsupportedDeadLetterProperties() throws Exception {
        Message message = mock(Message.class);
        MessageReference reference = mock(MessageReference.class);
        when(reference.getMessage()).thenReturn(message);
        Rejected rejected = new Rejected();
        ErrorCondition error = new ErrorCondition();
        Instant modifiedAt = Instant.parse("2026-08-28T00:00:00Z");
        error.setInfo(Map.of(
                Symbol.valueOf("modified-at"), Date.from(modifiedAt),
                Symbol.valueOf("attempt"), 3));
        rejected.setError(error);

        try (URLClassLoader loader = PatchJarLoader.open()) {
            support(loader).getMethod("apply", MessageReference.class, Rejected.class)
                    .invoke(null, reference, rejected);
        }

        verify(message).putObjectProperty("modified-at", modifiedAt.toString());
        verify(message).putObjectProperty("attempt", 3);
        verify(message).reencode();
    }

    @Test
    void aClientDeadLetterKeepsTheReasonItGaveAndTheDeliveriesThatFailed() throws Exception {
        AMQPMessage stored = storedMessage();
        // Abandoned once, then dead-lettered on its second delivery, which Azure does not count.
        MessageReference reference = reference(stored, 2);
        Rejected rejected = new Rejected();
        ErrorCondition error = new ErrorCondition();
        error.setInfo(Map.of(
                Symbol.valueOf("DeadLetterReason"), "poison",
                Symbol.valueOf("DeadLetterErrorDescription"), "unreadable"));
        rejected.setError(error);

        try (URLClassLoader loader = PatchJarLoader.open()) {
            support(loader).getMethod("apply", MessageReference.class, Rejected.class)
                    .invoke(null, reference, rejected);
        }

        Message deadLetter = stored.copy(DEAD_LETTER_ID, true);
        assertEquals("poison", deadLetter.getStringProperty("DeadLetterReason"));
        assertEquals("unreadable", deadLetter.getStringProperty("DeadLetterErrorDescription"));
        assertEquals(1, ((AMQPMessage) deadLetter).getExtraProperties().getProperty(PRIOR_DELIVERY_COUNT));
    }

    @Test
    void aFailedDeliveryThatReachesMaxDeliveryCountGivesAzuresReason() throws Exception {
        AMQPMessage stored = storedMessage();

        try (URLClassLoader loader = PatchJarLoader.open()) {
            beforeFailedDelivery(loader, reference(stored, MAX_DELIVERY_COUNT), MAX_DELIVERY_COUNT);
        }

        // Artemis dead-letters a copy of the message, so it is the copy that has to carry them.
        Message deadLetter = stored.copy(DEAD_LETTER_ID, true);
        assertEquals("MaxDeliveryCountExceeded", deadLetter.getStringProperty("DeadLetterReason"));
        assertEquals("Message could not be consumed after 10 delivery attempts.",
                deadLetter.getStringProperty("DeadLetterErrorDescription"));
        assertEquals(MAX_DELIVERY_COUNT,
                ((AMQPMessage) deadLetter).getExtraProperties().getProperty(PRIOR_DELIVERY_COUNT));
        assertEquals("kept", deadLetter.getStringProperty("custom"));
    }

    @ParameterizedTest
    @CsvSource({
            // Below the limit Artemis returns the message to its queue.
            "floci-az:servicebus-peeklock:60, 9, 10",
            // Artemis never dead-letters a message of an entity without a limit.
            "floci-az:servicebus-peeklock:60, 10, -1",
            // Event Hubs consumers share the AMQP layer.
            ", 10, 10"})
    void aFailedDeliveryArtemisReturnsToItsQueueIsUnchanged(String queueUser, int deliveryCount,
                                                            int maxDeliveryCount) throws Exception {
        AMQPMessage stored = storedMessage();
        MessageReference reference = reference(stored, deliveryCount);
        when(reference.getQueue().getUser()).thenReturn(queueUser == null ? null : SimpleString.of(queueUser));

        try (URLClassLoader loader = PatchJarLoader.open()) {
            beforeFailedDelivery(loader, reference, maxDeliveryCount);
        }

        assertNull(stored.getStringProperty("DeadLetterReason"));
        assertNull(stored.getExtraProperties());
    }

    private static Class<?> support(URLClassLoader loader) throws ClassNotFoundException {
        return Class.forName(
                "org.apache.activemq.artemis.protocol.amqp.proton.ServiceBusDeadLetterSupport", true, loader);
    }

    private static void beforeFailedDelivery(URLClassLoader loader, MessageReference reference,
                                             int maxDeliveryCount) throws Exception {
        ActiveMQServer server = mock(ActiveMQServer.class, RETURNS_DEEP_STUBS);
        AddressSettings settings = new AddressSettings().setMaxDeliveryAttempts(maxDeliveryCount);
        when(server.getAddressSettingsRepository().getMatch(ENTITY)).thenReturn(settings);
        support(loader).getMethod("beforeFailedDelivery", ActiveMQServer.class, MessageReference.class)
                .invoke(null, server, reference);
    }

    private static MessageReference reference(AMQPMessage message, int deliveryCount) {
        MessageReference reference = mock(MessageReference.class, RETURNS_DEEP_STUBS);
        when(reference.getMessage()).thenReturn(message);
        when(reference.getDeliveryCount()).thenReturn(deliveryCount);
        when(reference.getQueue().getUser()).thenReturn(SimpleString.of(PEEK_LOCK_QUEUE));
        when(reference.getQueue().getAddress()).thenReturn(SimpleString.of(ENTITY));
        return reference;
    }

    private static AMQPMessage storedMessage() {
        // Proton's Message, which clashes with Artemis's.
        org.apache.qpid.proton.message.Message original = Proton.message();
        original.setMessageId("message-id");
        original.setBody(new AmqpValue("payload"));
        original.setApplicationProperties(new ApplicationProperties(Map.of("custom", "kept")));
        byte[] encoded = new byte[1024];
        int size = original.encode(encoded, 0, encoded.length);
        return new AMQPStandardMessage(0, Arrays.copyOf(encoded, size), null);
    }
}
