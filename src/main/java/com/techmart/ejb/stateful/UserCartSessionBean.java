package com.techmart.ejb.stateful;

import com.techmart.model.CartItem;
import com.techmart.model.Order;
import com.techmart.model.OrderItem;
import com.techmart.util.PerformanceMonitor;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.ejb.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.logging.Logger;

/**
 * Stateful Session Bean — Shopping Cart.
 *
 * WHY STATEFUL?
 * A shopping cart must remember each customer's selections across multiple
 * HTTP requests (browsing, adding items, reviewing) within the same session.
 * A stateless bean is re-pooled between calls and cannot hold this state.
 * A stateful bean is bound to a single client for the duration of the session.
 *
 * LIFECYCLE:
 *   Does Not Exist
 *     → @PostConstruct (cart initialised)
 *     → Ready (client calls methods)
 *     → [Passivated] (container swaps to disk under memory pressure)
 *     → @PrePassivate called before passivation
 *     → @PostActivate called after restoration
 *     → @Remove — client calls checkout() or clearCart(), bean destroyed
 *     → @PreDestroy
 *
 * PERFORMANCE CONSIDERATION:
 * Each active stateful bean occupies heap memory. At 10,000 concurrent users
 * this is significant. Mitigations:
 *   - Set statefulTimeout to expire idle carts (e.g., 30 min)
 *   - Keep cart data lightweight (IDs + quantities, fetch details on demand)
 *   - Use distributed session replication (Shoal/Hazelcast in GlassFish) for HA
 *
 * @StatefulTimeout ensures the container removes idle beans automatically,
 * freeing memory without requiring explicit client cleanup.
 */
@Stateful
@StatefulTimeout(value = 30, unit = java.util.concurrent.TimeUnit.MINUTES)
public class UserCartSessionBean {

    private static final Logger LOG = Logger.getLogger(UserCartSessionBean.class.getName());
    private static final String COMPONENT = "UserCartSessionBean";

    @EJB
    private PerformanceMonitor perfMonitor;

    /** The cart contents: productId → CartItem */
    private Map<Long, CartItem> cartItems;

    /** Customer this cart belongs to */
    private String customerId;

    /** Timestamp of last modification for idle detection */
    private long lastModified;

    @PostConstruct
    public void init() {
        cartItems = new LinkedHashMap<>();   // LinkedHashMap preserves insertion order
        lastModified = System.currentTimeMillis();
        LOG.info("Shopping cart initialised for new session");
    }

    @PreDestroy
    public void destroy() {
        LOG.info("Shopping cart destroyed for customer: " + customerId);
        cartItems.clear();
    }

    /**
     * Called by the container before passivating this bean to disk.
     * Release any non-serializable resources (open connections, etc.).
     */
    @PrePassivate
    public void onPassivate() {
        // CartItem is Serializable — nothing to release
        LOG.fine("Cart passivated for customer: " + customerId);
    }

    /**
     * Called by the container after restoring a passivated bean.
     * Re-acquire resources released in @PrePassivate.
     */
    @PostActivate
    public void onActivate() {
        LOG.fine("Cart activated for customer: " + customerId);
    }

    // ------------------------------------------------------------------
    // Business Methods
    // ------------------------------------------------------------------

    /**
     * Associates this cart with a customer.
     * Called after the customer logs in.
     */
    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getCustomerId() {
        return customerId;
    }

    /**
     * Adds a product to the cart. If already present, increments quantity.
     *
     * @param productId  database PK of the product
     * @param sku        product SKU (for display)
     * @param name       product name (for display)
     * @param quantity   how many to add
     * @param unitPrice  current price (snapshotted at add-time)
     */
    public void addItem(Long productId, String sku, String name,
                        int quantity, BigDecimal unitPrice) {
        long start = System.currentTimeMillis();
        try {
            if (cartItems.containsKey(productId)) {
                cartItems.get(productId).incrementQuantity(quantity);
                LOG.fine("Incremented qty for product " + sku);
            } else {
                cartItems.put(productId, new CartItem(productId, sku, name, quantity, unitPrice));
                LOG.fine("Added new item to cart: " + sku);
            }
            lastModified = System.currentTimeMillis();
        } finally {
            perfMonitor.record(COMPONENT, "addItem", System.currentTimeMillis() - start);
        }
    }

    /**
     * Removes a product from the cart entirely.
     */
    public void removeItem(Long productId) {
        long start = System.currentTimeMillis();
        try {
            cartItems.remove(productId);
            lastModified = System.currentTimeMillis();
        } finally {
            perfMonitor.record(COMPONENT, "removeItem", System.currentTimeMillis() - start);
        }
    }

    /**
     * Updates the quantity for an existing cart item.
     * If newQuantity <= 0, the item is removed.
     */
    public void updateQuantity(Long productId, int newQuantity) {
        if (newQuantity <= 0) {
            removeItem(productId);
            return;
        }
        CartItem item = cartItems.get(productId);
        if (item != null) {
            item.setQuantity(newQuantity);
            lastModified = System.currentTimeMillis();
        }
    }

    /**
     * Returns a read-only view of current cart contents.
     */
    public List<CartItem> getItems() {
        return Collections.unmodifiableList(new ArrayList<>(cartItems.values()));
    }

    /**
     * Calculates the cart grand total.
     */
    public BigDecimal getTotal() {
        return cartItems.values().stream()
                .map(CartItem::getSubtotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    /**
     * Returns true if the cart has no items.
     */
    public boolean isEmpty() {
        return cartItems.isEmpty();
    }

    public int getItemCount() {
        return cartItems.values().stream().mapToInt(CartItem::getQuantity).sum();
    }

    /**
     * Converts the cart into an Order ready for placement.
     * Does NOT clear the cart — call clearCart() or checkout() after
     * OrderServiceBean.placeOrder() confirms success.
     *
     * @param customerEmail  required for order confirmation email
     * @param shippingAddress  delivery address
     */
    public Order buildOrder(String customerEmail, String shippingAddress) {
        if (cartItems.isEmpty()) {
            throw new IllegalStateException("Cannot build an order from an empty cart");
        }

        Order order = new Order(customerId, customerEmail);
        order.setShippingAddress(shippingAddress);

        for (CartItem cartItem : cartItems.values()) {
            OrderItem orderItem = new OrderItem(
                cartItem.getProductId(),
                cartItem.getSku(),
                cartItem.getName(),
                cartItem.getQuantity(),
                cartItem.getUnitPrice()
            );
            order.addItem(orderItem);
        }

        return order;
    }

    /**
     * Empties the cart and marks this bean for removal.
     * The @Remove annotation signals the container to destroy this
     * stateful bean after the method returns, freeing its memory.
     */
    @Remove
    public void checkout() {
        LOG.info("Checkout complete — cart cleared and bean removed for customer: " + customerId);
        cartItems.clear();
    }

    /**
     * Clears the cart without destroying the bean (customer wants to
     * keep their session but abandon the current basket).
     */
    @Remove
    public void clearCart() {
        cartItems.clear();
        lastModified = System.currentTimeMillis();
        LOG.info("Cart cleared (session preserved) for customer: " + customerId);
    }

    public long getLastModified() {
        return lastModified;
    }
}
