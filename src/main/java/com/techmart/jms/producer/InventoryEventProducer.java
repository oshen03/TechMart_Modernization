package com.techmart.jms.producer;

import com.techmart.util.PerformanceMonitor;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.jms.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JMS Producer — Inventory Topic (Publish-Subscribe).
 *
 * PATTERN: Publish-Subscribe (Topic)
 *   An inventory change event is broadcast to ALL subscribers simultaneously.
 *   This is correct for warehouse synchronisation — every warehouse node
 *   must receive every stock change regardless of how many nodes exist.
 *
 * JMS PARTICIPANTS:
 *   Publisher   → this bean (InventoryEventProducer)
 *   Broker      → GlassFish embedded Open MQ broker
 *   Subscribers → InventoryUpdateMDB instances on every cluster node,
 *                 plus any external warehouse management systems
 *   Message     → MapMessage with productId, newLevel, eventType fields
 *
 * WHY MapMessage HERE (vs ObjectMessage for orders)?
 *   Inventory events are simple key-value payloads. MapMessage avoids
 *   serialisation overhead of ObjectMessage and is more interoperable
 *   with non-Java subscribers (e.g., a Python warehouse dashboard).
 *
 * DURABLE SUBSCRIPTIONS:
 *   If a warehouse node is temporarily offline, it could miss events.
 *   Durable subscriptions (createDurableConsumer) ensure the broker
 *   retains messages until each named subscriber acknowledges receipt.
 *   The InventoryUpdateMDB uses activationConfig to request durable subscription.
 */
@Stateless
public class InventoryEventProducer {

    private static final Logger LOG = Logger.getLogger(InventoryEventProducer.class.getName());
    private static final String COMPONENT = "InventoryEventProducer";

    @Resource(lookup = "jms/ConnectionFactory")
    private ConnectionFactory connectionFactory;

    /** Inventory topic for pub/sub broadcast */
    @Resource(lookup = "jms/InventoryTopic")
    private Topic inventoryTopic;

    @EJB
    private PerformanceMonitor perfMonitor;

    @PostConstruct
    public void init() {
        LOG.info("InventoryEventProducer ready — targeting topic: jms/InventoryTopic");
    }

    /**
     * Publishes an inventory level change to all warehouse subscribers.
     *
     * Uses NON_PERSISTENT delivery for inventory events because:
     * - Events arrive frequently (every reservation / restock)
     * - If one event is lost, the next event self-corrects the level
     * - NON_PERSISTENT avoids the journal write overhead of PERSISTENT
     *
     * For financial or audit-critical events, switch to PERSISTENT.
     *
     * @param productId  the product whose stock changed
     * @param newLevel   the authoritative new stock level
     * @param eventType  "RESERVE", "RESTOCK", or "ADJUSTMENT"
     */
    public void publishInventoryUpdate(Long productId, int newLevel, String eventType) {
        long start = System.currentTimeMillis();

        try (JMSContext context = connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {

            MapMessage message = context.createMapMessage();
            message.setLong("productId", productId);
            message.setInt("newLevel", newLevel);
            message.setString("eventType", eventType);
            message.setLong("timestamp", System.currentTimeMillis());

            // Message selector property — subscribers can filter by eventType
            message.setStringProperty("eventType", eventType);

            context.createProducer()
                   .setDeliveryMode(DeliveryMode.NON_PERSISTENT)
                   .setTimeToLive(60_000L)   // 1 minute TTL — stale inventory events are useless
                   .send(inventoryTopic, message);

            LOG.fine("Inventory event published: product=" + productId +
                     ", level=" + newLevel + ", type=" + eventType);

        } catch (JMSException e) {
            LOG.log(Level.WARNING,
                    "Failed to publish inventory event for product " + productId, e);
            // Non-fatal — local cache is already updated; log for monitoring
            throw new RuntimeException("Inventory publish failed", e);
        } finally {
            perfMonitor.record(COMPONENT, "publishInventoryUpdate",
                               System.currentTimeMillis() - start);
        }
    }
}
