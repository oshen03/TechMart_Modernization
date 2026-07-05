package com.techmart.jms.mdb;

import com.techmart.ejb.singleton.InventoryTrackerBean;
import com.techmart.ejb.stateless.OrderServiceBean;
import com.techmart.model.Order;
import com.techmart.model.OrderItem;
import com.techmart.util.PerformanceMonitor;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.ejb.ActivationConfigProperty;
import jakarta.ejb.EJB;
import jakarta.ejb.MessageDriven;
import jakarta.jms.*;
import java.util.logging.Level;
import java.util.logging.Logger;


@MessageDriven(
    activationConfig = {
        @ActivationConfigProperty(
            propertyName  = "destinationType",
            propertyValue = "jakarta.jms.Queue"
        ),
        @ActivationConfigProperty(
            // GlassFish uses destinationLookup (JNDI) rather than
            // the physical destination name used by some other containers.
            propertyName  = "destinationLookup",
            propertyValue = "jms/OrderQueue"
        ),
        @ActivationConfigProperty(
            propertyName  = "acknowledgeMode",
            propertyValue = "Auto-acknowledge"
        ),
        @ActivationConfigProperty(
            // GlassFish Open MQ: max redelivery attempts before dead message queue
            propertyName  = "endpointExceptionRedeliveryAttempts",
            propertyValue = "3"
        ),
        @ActivationConfigProperty(
            propertyName  = "maxSession",
            propertyValue = "10"
        )
    }
)
public class OrderProcessorMDB implements MessageListener {

    private static final Logger LOG = Logger.getLogger(OrderProcessorMDB.class.getName());
    private static final String COMPONENT = "OrderProcessorMDB";

    @EJB
    private InventoryTrackerBean inventoryTracker;

    @EJB
    private OrderServiceBean orderService;

    @EJB
    private PerformanceMonitor perfMonitor;

    @PostConstruct
    public void init() {
        LOG.info("[MDB-POOL-ENTRY] OrderProcessorMDB instance provisioned. Actively listening on point-to-point destination: jms/OrderQueue");
    }

    @PreDestroy
    public void destroy() {
        LOG.info("OrderProcessorMDB instance removed from pool");
    }

    /**
     * Main message handler. Invoked by the container for each Order message
     * dequeued from OrderQueue.
     *
     * Transaction behaviour: The container starts a new JTA transaction before
     * calling onMessage(). If this method returns normally, the transaction
     * commits and the message is acknowledged. If a RuntimeException is thrown,
     * the transaction rolls back and the message is redelivered.
     *
     * Checked exceptions (e.g., JMSException) must be caught and either
     * wrapped as RuntimeException (to trigger rollback/redeliver) or
     * handled gracefully (to commit and move on).
     */
    @Override
    public void onMessage(Message message) {
        long start = System.currentTimeMillis();
        Long orderId = null;

        try {
            LOG.info(String.format("[JMS-CONSUMER] [QUEUE-RECEIVE] Dequeued ObjectMessage from broker. Starting background fulfillment pipeline for Order Reference ID: %d", orderId));
            if (!(message instanceof ObjectMessage)) {
                LOG.warning("Unexpected message type received: " + message.getClass().getName());
                return; // Acknowledge and discard — wrong message type
            }

            ObjectMessage objectMessage = (ObjectMessage) message;
            Order order = (Order) objectMessage.getObject();
            orderId = order.getId();

            LOG.info("Processing order: " + orderId + " for customer: " + order.getCustomerId());

            // Step 1 — Reserve inventory for every line item
            boolean allReserved = reserveInventoryForOrder(order);

            if (allReserved) {
                // Step 2a — All stock available: advance to PROCESSING
                orderService.updateOrderStatus(
                    orderId,
                    Order.Status.PROCESSING,
                    "Inventory reserved. Order dispatched to fulfilment."
                );
                LOG.info(String.format("[FULFILLMENT-PIPELINE] [STOCK-CONFIRMED] Multi-warehouse inventory allocations validated. Moving Order ID: %d state vector to PROCESSING.", orderId));

            } else {
                // Step 2b — Insufficient stock: hold order for review
                orderService.updateOrderStatus(
                    orderId,
                    Order.Status.CANCELLED,
                    "Order cancelled: insufficient stock for one or more items."
                );
                LOG.severe(String.format("[FULFILLMENT-PIPELINE] [STOCK-EXCEPTION] Insufficient inventory thresholds detected for order elements. Aborting fulfillment. Order ID: %d transitioned to CANCELLED state.", orderId));
            }

        } catch (JMSException e) {
            LOG.log(Level.SEVERE, "JMS error processing order " + orderId, e);
            // Throw RuntimeException to trigger tx rollback and message redeliver
            throw new RuntimeException("JMS deserialization failed", e);

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Unexpected error processing order " + orderId, e);
            throw new RuntimeException("Order processing failed", e);

        } finally {
            perfMonitor.record(COMPONENT, "onMessage", System.currentTimeMillis() - start);
        }
    }

    /**
     * Attempts to reserve inventory for every item in the order.
     * If any item fails, already-reserved stock is returned (rollback logic).
     *
     * @return true if all items were reserved successfully
     */
    private boolean reserveInventoryForOrder(Order order) {
        java.util.List<OrderItem> reservedSoFar = new java.util.ArrayList<>();

        for (OrderItem item : order.getItems()) {
            boolean reserved = inventoryTracker.reserveStock(
                item.getProductId(), item.getQuantity()
            );

            if (!reserved) {
                // Compensating transaction — return what we already reserved
                LOG.warning("Stock unavailable for product " + item.getProductSku() +
                            " (qty=" + item.getQuantity() + "). Rolling back reserved items.");
                for (OrderItem alreadyReserved : reservedSoFar) {
                    inventoryTracker.restockProduct(
                        alreadyReserved.getProductId(), alreadyReserved.getQuantity()
                    );
                }
                return false;
            }

            reservedSoFar.add(item);
        }

        return true;
    }
}
