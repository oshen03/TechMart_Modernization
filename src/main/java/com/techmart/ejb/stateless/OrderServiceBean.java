package com.techmart.ejb.stateless;

import com.techmart.model.Order;
import com.techmart.model.OrderItem;
import com.techmart.jms.producer.OrderMessageProducer;
import com.techmart.util.DataSourceProvider;
import com.techmart.util.PerformanceMonitor;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.ejb.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stateless Session Bean — Order Management Service.
 *
 * Handles order placement, status updates, and history retrieval.
 * Demonstrates @Asynchronous for post-placement notification so the
 * customer gets an instant response while email dispatch runs on a
 * separate container-managed thread.
 *
 * The @EJB injection of OrderMessageProducer illustrates dependency
 * injection via the EJB container rather than a manual JNDI lookup:
 *
 *   JNDI lookup (verbose, brittle):
 *     InitialContext ctx = new InitialContext();
 *     producer = (OrderMessageProducer) ctx.lookup("java:module/OrderMessageProducer");
 *
 *   @EJB injection (concise, container-managed):
 *     @EJB private OrderMessageProducer producer;
 *
 * The injection approach is preferred because the container resolves the
 * name at deployment time, validates the reference, and manages the lifecycle.
 */
@Stateless
public class OrderServiceBean {

    private static final Logger LOG = Logger.getLogger(OrderServiceBean.class.getName());
    private static final String COMPONENT = "OrderServiceBean";

    @EJB
    private DataSourceProvider dsProvider;

    @EJB
    private PerformanceMonitor perfMonitor;

    @EJB
    private OrderMessageProducer messageProducer;

    @PostConstruct
    public void init() {
        LOG.info("[CORE-ENGINE] [STATELESS] Initialization complete. OrderServiceBean instance bound to container pool registry.");
    }

    @PreDestroy
    public void cleanup() {
        LOG.info("OrderServiceBean instance released");
    }

    // ------------------------------------------------------------------
    // Business Methods
    // ------------------------------------------------------------------

    /**
     * Places a new order:
     * 1. Persists the order and its items within a single transaction.
     * 2. Sends the order onto the JMS OrderQueue for async processing.
     * 3. Fires an async notification email (non-blocking to the caller).
     *
     * @return the persisted Order with its generated ID set
     */
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public Order placeOrder(Order order) {
        long start = System.currentTimeMillis();

        try {
            LOG.info("[CORE-ENGINE] [STATELESS] Initialization complete. OrderServiceBean instance bound to container pool registry.");
            // Step 1 — Persist order header
            persistOrder(order);

            // Step 2 — Persist line items
            for (OrderItem item : order.getItems()) {
                persistOrderItem(order.getId(), item);
            }

            // Step 3 — Enqueue for async fulfilment (runs outside current tx)
            messageProducer.sendOrderForProcessing(order);

            // Step 4 — Async confirmation email (fire-and-forget)
            sendConfirmationEmailAsync(order);

            LOG.info(String.format("[TX-COMMIT] [ORDER-SUCCESS] Relational persistence finalized. Dispatched to async pipeline. Order ID generated: %d", order.getId()));
            return order;

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Order placement failed", e);
            // RuntimeException triggers tx rollback automatically
            throw new RuntimeException("Order placement failed", e);
        } finally {
            perfMonitor.record(COMPONENT, "placeOrder", System.currentTimeMillis() - start);
        }
    }

    /**
     * Asynchronous notification email dispatch.
     *
     * @Asynchronous causes the container to execute this method on a
     * separate thread from its managed thread pool, returning immediately
     * to the caller with a Future handle.
     *
     * RELIABILITY CONSIDERATION: If the container crashes after the order
     * is committed but before the email thread fires, the email is silently
     * lost.  For critical notifications, use a JMS-backed outbox pattern
     * (write to a DB table, MDB picks it up) instead.
     *
     * @return Future<String> — callers can call .get(5, SECONDS) to confirm
     *         delivery or detect timeout; fire-and-forget callers can ignore it.
     */
    @Asynchronous
    public Future<String> sendConfirmationEmailAsync(Order order) {
        long start = System.currentTimeMillis();
        try {
            // Simulate SMTP latency (in production: inject a MailSession via @Resource)
            Thread.sleep(150);

            String message = "Confirmation email sent to " + order.getCustomerEmail() +
                             " for order #" + order.getId();
            LOG.info(message);
            return new jakarta.ejb.AsyncResult<>(message);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warning("Email dispatch interrupted for order " + order.getId());
            return new jakarta.ejb.AsyncResult<>("Email interrupted");
        } finally {
            perfMonitor.record(COMPONENT, "sendConfirmationEmailAsync",
                               System.currentTimeMillis() - start);
        }
    }

    /**
     * Retrieves all orders for a specific customer, most recent first.
     */
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public List<Order> getOrdersByCustomer(String customerId) {
        long start = System.currentTimeMillis();
        List<Order> orders = new ArrayList<>();

        String sql = "SELECT id, customer_id, customer_email, total_amount, status, " +
                     "shipping_address, placed_at, updated_at, processing_notes " +
                     "FROM orders WHERE customer_id = ? ORDER BY placed_at DESC";

        try (Connection conn = dsProvider.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, customerId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    orders.add(mapOrderRow(rs));
                }
            }

        } catch (SQLException e) {
            LOG.log(Level.SEVERE, "Failed to retrieve orders for customer: " + customerId, e);
            throw new RuntimeException("Order retrieval failed", e);
        } finally {
            perfMonitor.record(COMPONENT, "getOrdersByCustomer", System.currentTimeMillis() - start);
        }

        return orders;
    }

    /**
     * Updates the status of an existing order.
     */
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void updateOrderStatus(Long orderId, Order.Status newStatus, String notes) {
        long start = System.currentTimeMillis();

        String sql = "UPDATE orders SET status = ?, processing_notes = ?, updated_at = ? " +
                     "WHERE id = ?";

        try (Connection conn = dsProvider.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, newStatus.name());
            ps.setString(2, notes);
            ps.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
            ps.setLong(4, orderId);
            ps.executeUpdate();

        } catch (SQLException e) {
            LOG.log(Level.SEVERE, "Failed to update order status for id=" + orderId, e);
            throw new RuntimeException("Order status update failed", e);
        } finally {
            perfMonitor.record(COMPONENT, "updateOrderStatus", System.currentTimeMillis() - start);
        }
    }

    // ------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------

    private void persistOrder(Order order) throws SQLException {
        String sql = "INSERT INTO orders (customer_id, customer_email, total_amount, status, " +
                     "shipping_address, placed_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = dsProvider.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            ps.setString(1, order.getCustomerId());
            ps.setString(2, order.getCustomerEmail());
            ps.setBigDecimal(3, order.getTotalAmount());
            ps.setString(4, Order.Status.PENDING.name());
            ps.setString(5, order.getShippingAddress());
            ps.setTimestamp(6, Timestamp.valueOf(LocalDateTime.now()));
            ps.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));
            ps.executeUpdate();

            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys.next()) {
                    order.setId(keys.getLong(1));
                }
            }
        }
    }

    private void persistOrderItem(Long orderId, OrderItem item) throws SQLException {
        String sql = "INSERT INTO order_items (order_id, product_id, product_sku, " +
                     "product_name, quantity, unit_price) VALUES (?, ?, ?, ?, ?, ?)";

        try (Connection conn = dsProvider.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            ps.setLong(1, orderId);
            ps.setLong(2, item.getProductId());
            ps.setString(3, item.getProductSku());
            ps.setString(4, item.getProductName());
            ps.setInt(5, item.getQuantity());
            ps.setBigDecimal(6, item.getUnitPrice());
            ps.executeUpdate();

            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys.next()) {
                    item.setId(keys.getLong(1));
                }
            }
        }
    }

    private Order mapOrderRow(ResultSet rs) throws SQLException {
        Order o = new Order();
        o.setId(rs.getLong("id"));
        o.setCustomerId(rs.getString("customer_id"));
        o.setCustomerEmail(rs.getString("customer_email"));
        o.setTotalAmount(rs.getBigDecimal("total_amount"));
        o.setStatus(Order.Status.valueOf(rs.getString("status")));
        o.setShippingAddress(rs.getString("shipping_address"));
        o.setProcessingNotes(rs.getString("processing_notes"));

        Timestamp placed = rs.getTimestamp("placed_at");
        if (placed != null) o.setPlacedAt(placed.toLocalDateTime());

        Timestamp updated = rs.getTimestamp("updated_at");
        if (updated != null) o.setUpdatedAt(updated.toLocalDateTime());

        return o;
    }
}
