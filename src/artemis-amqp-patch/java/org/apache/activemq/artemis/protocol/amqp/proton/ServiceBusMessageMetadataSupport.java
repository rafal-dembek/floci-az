package org.apache.activemq.artemis.protocol.amqp.proton;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;

/** Projects broker metadata onto outgoing Service Bus messages, independently of message locks. */
public final class ServiceBusMessageMetadataSupport {
   private static final Symbol SEQUENCE_NUMBER = Symbol.valueOf("x-opt-sequence-number");
   private static final Symbol ENQUEUED_TIME = Symbol.valueOf("x-opt-enqueued-time");
   private static final SimpleString PEEK_LOCK_PREFIX = SimpleString.of("floci-az:servicebus-peeklock:");
   private static final SimpleString SESSION_PREFIX = SimpleString.of("floci-az:servicebus-session:");
   /**
    * The deliveries a message had before it was dead-lettered. Artemis dead-letters a copy, whose
    * reference counts from zero, where Azure keeps counting. A broker property: clients never see it.
    */
   public static final SimpleString PRIOR_DELIVERY_COUNT = SimpleString.of("floci_prior_delivery_count");

   private ServiceBusMessageMetadataSupport() {
   }

   /** The deliveries Azure counts for the message: on its queue, and before it was dead-lettered. */
   public static int deliveryCount(MessageReference reference) {
      int deliveries = reference.getDeliveryCount();
      if (reference.getMessage() instanceof AMQPMessage message && message.getExtraProperties() != null
         && message.getExtraProperties().getProperty(PRIOR_DELIVERY_COUNT) instanceof Integer prior) {
         return prior + deliveries;
      }
      return deliveries;
   }

   /** Keeps the delivery count Azure gives a message Artemis is about to dead-letter. */
   static void keepDeliveryCount(MessageReference reference, int deliveryCount) {
      if (reference.getMessage() instanceof AMQPMessage message) {
         message.createExtraProperties().putIntProperty(PRIOR_DELIVERY_COUNT, deliveryCount);
      }
   }

   public static boolean isServiceBus(MessageReference reference) {
      SimpleString user = reference.getQueue().getUser();
      return user != null && (user.startsWith(PEEK_LOCK_PREFIX) || user.startsWith(SESSION_PREFIX));
   }

   /** Copy standard messages so delivery-specific annotations never enter shared broker state. */
   public static AMQPMessage forDelivery(AMQPMessage message, MessageReference reference) {
      if (!isServiceBus(reference)) {
         return message;
      }
      AMQPMessage copy = (AMQPMessage) message.copy();
      MessageAnnotations annotations = annotationsForDelivery(null, message, reference);
      annotations.getValue().forEach((key, value) -> copy.setAnnotation(SimpleString.of(key.toString()), value));
      copy.reencode();
      return copy;
   }

   /** Large messages retain their streamed body; only their outgoing annotations are replaced. */
   public static MessageAnnotations annotationsForDelivery(
      MessageAnnotations original, AMQPMessage message, MessageReference reference) {
      if (!isServiceBus(reference)) {
         return original;
      }
      Map<Symbol, Object> annotations = original == null ? new HashMap<>() : new HashMap<>(original.getValue());
      // Match what a peek reports (ServiceBusManagementPlugin): entity-local identity and original
      // broker ingress time.
      annotations.put(SEQUENCE_NUMBER, reference.getMessageID());
      annotations.put(ENQUEUED_TIME, new Date(enqueuedTime(message)));
      return ServiceBusMessageLockSupport.annotationsForDelivery(new MessageAnnotations(annotations), reference);
   }

   /** Every Service Bus delivery needs a count, including sessions and ReceiveAndDelete. */
   public static Header headerForDelivery(Header original, MessageReference reference) {
      if (!isServiceBus(reference)) {
         return original;
      }
      Header header = original == null ? new Header() : new Header(original);
      header.setDeliveryCount(UnsignedInteger.valueOf(Math.max(0, deliveryCount(reference) - 1)));
      return header;
   }

   private static long enqueuedTime(AMQPMessage message) {
      Long ingressTimestamp = message.getIngressTimestamp();
      if (ingressTimestamp != null) {
         return ingressTimestamp;
      }
      long creationTime = message.getTimestamp();
      if (creationTime > 0) {
         return creationTime;
      }
      throw new IllegalStateException("Service Bus message is missing an ingress timestamp");
   }
}
