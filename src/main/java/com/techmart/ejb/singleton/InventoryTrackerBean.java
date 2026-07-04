package com.techmart.ejb.singleton;

import com.techmart.jms.producer.InventoryEventProducer;
import com.techmart.util.DataSourceProvider;
import com.techmart.util.PerformanceMonitor;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.ejb.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Singleton Session Bean — Real-Time Inventory Tracker.
 *
 * WHY SINGLETON?
 * Inventory counts must be consistent across ALL concurrent requests.
 * If product SKU-1001 has 5 units and two users simultaneously try to
 * buy 3 each, a stateless pool would let both succeed (overselling).
 * A singleton guarantees one authoritative inventory state per JVM.
 *
 * LIFECYCLE:
 *   Does Not Exist
 *     → @PostConstruct (loads inventory from DB into in-memory cache)
 *     → Ready (single instance, container-managed concurrency)
 *     → @PreDestroy (flushes dirty counts to DB on shutdown)
 *
 * CONCURRENCY STRATEGY:
 *   Using @ConcurrencyManagement(CONTAINER) with @Lock annotations:
 *   - @Lock(READ)  → multiple threads may read simultaneously
 *   - @Lock(WRITE) → exclusive access for mutations (reserve/restock)
 *
 *   Container-managed concurrency is simpler and safer than BEAN-managed
 *   (which would require manual synchronized blocks or ReentrantReadWriteLock).
 *
 * CLUSTERING NOTE:
 *   This singleton only guarantees consistency within a single JVM.
 *   In a GlassFish cluster, each node has its own singleton instance.
 *   For cross-node consistency, emit inventory change events via JMS
 *   (InventoryEventProducer → InventoryUpdateMDB on every node).
 *   This is implemented below via publishInventoryChange().
 */
@Singleton
@Startup
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
public class InventoryTrackerBean {

    private static final Logger LOG = Logger.getLogger(InventoryTrackerBean.class.getName());
    private static final String COMPONENT = "InventoryTrackerBean";

    @EJB
    private DataSourceProvider dsProvider;

    @EJB
    private PerformanceMonitor perfMonitor;

    @EJB
    private InventoryEventProducer inventoryProducer;

    /**
     * In-memory cache: productId → available stock quantity.
     * Loaded at startup from the DB; updated in real-time on reserve/restock.
     */
    private final Map<Long, Integer> inventoryCache = new HashMap<>();

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /**
     * Loads the current inventory from the database into the in-memory cache.
     * Called once when GlassFish starts (because of @Startup).
     * Using @Lock(WRITE) here is implicit since no concurrent calls can
     * occur during @PostConstruct.
     */
    @PostConstruct
    public void loadInventory() {
        long start = System.currentTimeMillis();
        LOG.info("InventoryTrackerBean: loading inventory from database...");

        String sql = "SELECT id, stock_quantity FROM products";
        try (Connection conn = dsProvider.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            inventoryCache.clear();
            while (rs.next()) {
                inventoryCache.put(rs.getLong("id"), rs.getInt("stock_quantity"));
            }

            LOG.info("Inventory cache loaded: " + inventoryCache.size() + " products");

        } catch (SQLException e) {
            LOG.log(Level.SEVERE, "Failed to load inventory from DB", e);
            // Non-fatal at startup — DB might not be seeded yet
        } finally {
            perfMonitor.record(COMPONENT, "loadInventory", System.currentTimeMillis() - start);
        }
    }

    @PreDestroy
    public void flushInventory() {
        LOG.info("InventoryTrackerBean: flushing cache to DB on shutdown...");
        // In production: write all dirty entries back to the DB
        inventoryCache.clear();
    }

    // ------------------------------------------------------------------
    // Business Methods
    // ------------------------------------------------------------------

    /**
     * Returns the current in-memory stock level for a product.
     * Multiple threads may read concurrently — @Lock(READ) allows this.
     */
    @Lock(LockType.READ)
    public int getStockLevel(Long productId) {
        return inventoryCache.getOrDefault(productId, 0);
    }

    /**
     * Returns true if the requested quantity is available.
     */
    @Lock(LockType.READ)
    public boolean isAvailable(Long productId, int requestedQty) {
        return getStockLevel(productId) >= requestedQty;
    }

    /**
     * Returns the complete inventory snapshot (all products).
     */
    @Lock(LockType.READ)
    public Map<Long, Integer> getFullInventory() {
        return new HashMap<>(inventoryCache);  // defensive copy
    }

    /**
     * Attempts to reserve (decrement) stock for a product.
     *
     * @Lock(WRITE) grants exclusive access — no other thread can read or
     * write the cache while a reservation is in progress, preventing the
     * overselling race condition.
     *
     * @param productId    the product to reserve stock for
     * @param quantity     how many units to reserve
     * @return true if reservation succeeded; false if insufficient stock
     */
    @Lock(LockType.WRITE)
    public boolean reserveStock(Long productId, int quantity) {
        long start = System.currentTimeMillis();
        try {
            int current = inventoryCache.getOrDefault(productId, 0);

            if (current < quantity) {
                LOG.warning("Insufficient stock for product " + productId +
                            ": requested=" + quantity + ", available=" + current);
                return false;
            }

            int newLevel = current - quantity;
            inventoryCache.put(productId, newLevel);

            // Persist immediately to DB
            updateStockInDb(productId, newLevel);

            // Publish change to other nodes via JMS topic
            publishInventoryChange(productId, newLevel, "RESERVE");

            LOG.info("Stock reserved: product=" + productId + ", qty=" + quantity +
                     ", remaining=" + newLevel);
            return true;

        } finally {
            perfMonitor.record(COMPONENT, "reserveStock", System.currentTimeMillis() - start);
        }
    }

    /**
     * Restocks a product (returns reserved units or receives new shipment).
     *
     * @Lock(WRITE) for the same reason as reserveStock.
     */
    @Lock(LockType.WRITE)
    public void restockProduct(Long productId, int quantity) {
        long start = System.currentTimeMillis();
        try {
            int current = inventoryCache.getOrDefault(productId, 0);
            int newLevel = current + quantity;
            inventoryCache.put(productId, newLevel);

            updateStockInDb(productId, newLevel);
            publishInventoryChange(productId, newLevel, "RESTOCK");

            LOG.info("Product restocked: product=" + productId +
                     ", added=" + quantity + ", new level=" + newLevel);
        } finally {
            perfMonitor.record(COMPONENT, "restockProduct", System.currentTimeMillis() - start);
        }
    }

    /**
     * Receives an inventory update from another cluster node via MDB.
     * Updates only the local cache (DB already updated by the originating node).
     *
     * @Lock(WRITE) to prevent cache inconsistency during update.
     */
    @Lock(LockType.WRITE)
    public void applyRemoteInventoryUpdate(Long productId, int newLevel) {
        inventoryCache.put(productId, newLevel);
        LOG.fine("Applied remote inventory update: product=" + productId + ", level=" + newLevel);
    }

    // ------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------

    private void updateStockInDb(Long productId, int newLevel) {
        String sql = "UPDATE products SET stock_quantity = ?, updated_at = ? WHERE id = ?";
        try (Connection conn = dsProvider.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setInt(1, newLevel);
            ps.setTimestamp(2, Timestamp.valueOf(java.time.LocalDateTime.now()));
            ps.setLong(3, productId);
            ps.executeUpdate();

        } catch (SQLException e) {
            LOG.log(Level.SEVERE, "Failed to persist stock update for product " + productId, e);
        }
    }

    /**
     * Publishes an inventory change event to the JMS InventoryTopic so that
     * other cluster nodes can sync their in-memory caches via InventoryUpdateMDB.
     */
    private void publishInventoryChange(Long productId, int newLevel, String eventType) {
        try {
            inventoryProducer.publishInventoryUpdate(productId, newLevel, eventType);
        } catch (Exception e) {
            // Non-fatal — log and continue; cache is still consistent locally
            LOG.log(Level.WARNING,
                    "Failed to publish inventory event for product " + productId, e);
        }
    }
}
