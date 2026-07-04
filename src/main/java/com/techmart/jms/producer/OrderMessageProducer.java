package com.techmart.jms.producer;

import com.techmart.model.Order;
import com.techmart.util.PerformanceMonitor;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.jms.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JMS Producer — Order Queue (Point-to-Point).
 *
 * PATTERN: Point-to-Point (Queue)
 *   Each order message is consumed by exactly one OrderProcessorMDB instance.
 *   This is correct for order fulfilment — we do not want two MDB instances
 *   processing the same order simultaneously.
 *
 * JMS PARTICIPANTS:
 *   Producer  → this bean (OrderMessageProducer)
 *   Broker    → GlassFish's embedded Open MQ broker
 *   Consumer  → OrderProcessorMDB (listens on jms/OrderQueue)
 *   Message   → ObjectMessage carrying a serialised Order object
 *
 * WHY NOT TextMessage + JSON?
 *   ObjectMessage is simpler for internal EJB-to-MDB communication.
 *   For external integrations (REST APIs, cross-platform), TextMessage + JSON
 *   is preferred for interoperability.
 *
 * DELIVERY GUARANTEE:
 *   DeliveryMode.PERSISTENT ensures the broker writes the message to its
 *   journal before acknowledging send().  If the server crashes before the
 *   MDB consumes it, the message survives and is delivered on restart.
 *
 * DEAD LETTER HANDLING:
 *   GlassFish Open MQ sends undeliverable messages to DLQ/ExpiryQueue
 *   after maxDeliveryAttempts (default 10). Configure a separate MDB or
 *   monitoring job to inspect these.
 */
@Stateless
public class OrderMessageProducer {

    private static final Logger LOG = Logger.getLogger(OrderMessageProducer.class.getName());
    private static final String COMPONENT = "OrderMessageProducer";

    /**
     * JMS ConnectionFactory injected via @Resource (JNDI).
     * GlassFish provides jms/ConnectionFactory out of the box.
     */
    @Resource(lookup = "jms/ConnectionFactory")
    private ConnectionFactory connectionFactory;

    /**
     * Order queue injected via @Resource.
     * Defined in glassfish-resources.xml or via asadmin.
     */
    @Resource(lookup = "jms/OrderQueue")
    private Queue orderQueue;

    @EJB
    private PerformanceMonitor perfMonitor;

    @PostConstruct
    public void init() {
        LOG.info("OrderMessageProducer ready — targeting queue: jms/OrderQueue");
    }

    /**
     * Sends an Order onto the OrderQueue for asynchronous processing.
     *
     * Calling JMS 2.0 try-with-resources API automatically closes the
     * JMSContext (and its underlying Connection + Session) on scope exit,
     * returning resources to the connection pool cleanly.
     *
     * @param order  the Order to enqueue; must be Serializable
     */
    public void sendOrderForProcessing(Order order) {
        long start = System.currentTimeMillis();

        try (JMSContext context = connectionFactory.createContext(JMSContext.SESSION_TRANSACTED)) {

            JMSProducer producer = context.createProducer()
                    .setDeliveryMode(DeliveryMode.PERSISTENT)    // survives broker restart
                    .setTimeToLive(24 * 60 * 60 * 1000L)        // expire after 24 hours
                    .setPriority(Message.DEFAULT_PRIORITY);

            ObjectMessage message = context.createObjectMessage(order);

            // Custom properties allow MDB message selectors to filter
            message.setLongProperty("orderId", order.getId() != null ? order.getId() : -1L);
            message.setStringProperty("customerId", order.getCustomerId());
            message.setStringProperty("status", Order.Status.PENDING.name());

            producer.send(orderQueue, message);
            context.commit();   // commit the transacted session

            LOG.info("Order enqueued: orderId=" + order.getId() +
                     ", customer=" + order.getCustomerId());

        } catch (JMSException e) {
            LOG.log(Level.SEVERE, "Failed to enqueue order " + order.getId(), e);
            throw new RuntimeException("JMS send failed for order " + order.getId(), e);
        } finally {
            perfMonitor.record(COMPONENT, "sendOrderForProcessing",
                               System.currentTimeMillis() - start);
        }
    }
}
