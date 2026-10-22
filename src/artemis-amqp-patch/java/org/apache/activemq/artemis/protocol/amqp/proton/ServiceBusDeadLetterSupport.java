package org.apache.activemq.artemis.protocol.amqp.proton;

import java.util.Date;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQPropertyConversionException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;

/**
 * Applies Azure dead-letter metadata to a message before Artemis moves it to the dead-letter queue.
 *
 * <p>Artemis moves a copy of the message, so what is applied here travels with that copy. Only the
 * dead-lettered message is changed: each queue and subscription holds a message of its own.
 */
public final class ServiceBusDeadLetterSupport {

   private static final String DEAD_LETTER_REASON = "DeadLetterReason";
   private static final String DEAD_LETTER_ERROR_DESCRIPTION = "DeadLetterErrorDescription";
   /** Azure's reason and description for a message delivered its entity's MaxDeliveryCount times. */
   private static final String MAX_DELIVERY_COUNT_EXCEEDED = "MaxDeliveryCountExceeded";
   private static final String MAX_DELIVERY_COUNT_EXCEEDED_DESCRIPTION =
      "Message could not be consumed after %d delivery attempts.";

   private ServiceBusDeadLetterSupport() {
   }

   /** A client dead-letters the message, with the reason, description and properties its outcome carries. */
   public static void apply(MessageReference reference, Rejected rejected)
      throws ActiveMQPropertyConversionException {
      // Azure counts a delivery when its lock expires or it is abandoned, and the delivery that
      // dead-letters the message settles it.
      ServiceBusMessageMetadataSupport.keepDeliveryCount(
         reference, Math.max(0, ServiceBusMessageMetadataSupport.deliveryCount(reference) - 1));
      ErrorCondition error = rejected.getError();
      if (error == null || error.getInfo() == null) {
         return;
      }

      Message message = reference.getMessage();
      for (Object rawEntry : error.getInfo().entrySet()) {
         Map.Entry<?, ?> entry = (Map.Entry<?, ?>) rawEntry;
         message.putObjectProperty(entry.getKey().toString(), compatiblePropertyValue(entry.getValue()));
      }
      message.reencode();
   }

   /**
    * Called before a failed delivery (an abandon, or an expired lock) returns the message to its queue.
    * Artemis dead-letters it there once its delivery count reaches max-delivery-attempts, the entity's
    * MaxDeliveryCount (QueueImpl.checkRedelivery), but gives it no reason. This applies the same test
    * and, when it holds, the reason and description Azure gives.
    */
   public static void beforeFailedDelivery(ActiveMQServer server, MessageReference reference) {
      Queue queue = reference.getQueue();
      if (!ServiceBusMessageMetadataSupport.isServiceBus(reference) || queue.isInternalQueue()) {
         return;
      }
      int maxDeliveries = server.getAddressSettingsRepository()
         .getMatch(queue.getAddress().toString()).getMaxDeliveryAttempts();
      if (maxDeliveries <= 0 || reference.getDeliveryCount() < maxDeliveries) {
         return;
      }
      Message message = reference.getMessage();
      int deliveryCount = ServiceBusMessageMetadataSupport.deliveryCount(reference);
      message.putStringProperty(DEAD_LETTER_REASON, MAX_DELIVERY_COUNT_EXCEEDED);
      message.putStringProperty(DEAD_LETTER_ERROR_DESCRIPTION,
         String.format(MAX_DELIVERY_COUNT_EXCEEDED_DESCRIPTION, deliveryCount));
      ServiceBusMessageMetadataSupport.keepDeliveryCount(reference, deliveryCount);
      message.reencode();
   }

   private static Object compatiblePropertyValue(Object value) {
      if (value == null || value instanceof Boolean || value instanceof Byte
         || value instanceof Short || value instanceof Integer || value instanceof Long
         || value instanceof Float || value instanceof Double || value instanceof String
         || value instanceof SimpleString || value instanceof byte[]) {
         return value;
      }
      if (value instanceof Date date) {
         return date.toInstant().toString();
      }
      return value.toString();
   }
}
