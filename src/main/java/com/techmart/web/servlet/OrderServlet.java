package com.techmart.web.servlet;

import com.techmart.ejb.stateful.UserCartSessionBean;
import com.techmart.ejb.stateless.OrderServiceBean;
import com.techmart.ejb.stateless.ProductCatalogBean;
import com.techmart.model.Order;
import com.techmart.model.Product;

import jakarta.ejb.EJB;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.*;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servlet — Shopping Cart and Order Placement.
 *
 * Cart lifecycle:
 *   1. Customer adds item → UserCartSessionBean created and bound to HTTP session
 *   2. Customer views cart → reads bean state from session
 *   3. Customer checks out → OrderServiceBean.placeOrder() called,
 *      async email fires, cart bean @Remove'd, session attribute cleared
 *
 * The UserCartSessionBean is stored in the HTTP session (not injected with @EJB)
 * because @EJB-injected stateful beans are shared across all requests to
 * this servlet instance.  Each user needs their own UserCartSessionBean reference,
 * so we store it in HttpSession and look it up per request.
 *
 * In a full CDI application, @SessionScoped + @Inject would handle this
 * more elegantly, but the pure EJB approach is shown here per the assignment.
 *
 * URL mappings:
 *   GET  /cart             → view current cart
 *   POST /cart?action=add  → add item to cart
 *   POST /cart?action=remove → remove item
 *   POST /cart?action=checkout → place order
 *   GET  /orders           → view order history
 */
@WebServlet(name = "OrderServlet", urlPatterns = {"/cart", "/orders"})
public class OrderServlet extends HttpServlet {

    private static final Logger LOG = Logger.getLogger(OrderServlet.class.getName());
    private static final String CART_SESSION_KEY = "shoppingCart";

    @EJB
    private OrderServiceBean orderService;

    @EJB
    private ProductCatalogBean productCatalog;

    // ------------------------------------------------------------------
    // GET — View Cart or Order History
    // ------------------------------------------------------------------

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        String path = req.getServletPath();

        if ("/orders".equals(path)) {
            handleOrderHistory(req, resp);
        } else {
            handleViewCart(req, resp);
        }
    }

    private void handleViewCart(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        UserCartSessionBean cart = getOrCreateCart(req.getSession());
        req.setAttribute("cartItems", cart.getItems());
        req.setAttribute("cartTotal", cart.getTotal());
        req.setAttribute("itemCount", cart.getItemCount());
        req.getRequestDispatcher("/pages/cart.jsp").forward(req, resp);
    }

    private void handleOrderHistory(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        String customerId = getCustomerId(req.getSession());
        List<Order> orders = orderService.getOrdersByCustomer(customerId);
        req.setAttribute("orders", orders);
        req.getRequestDispatcher("/pages/orders.jsp").forward(req, resp);
    }

    // ------------------------------------------------------------------
    // POST — Cart Mutations and Checkout
    // ------------------------------------------------------------------

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        String action = req.getParameter("action");
        if (action == null) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing action parameter");
            return;
        }

        switch (action) {
            case "add":      handleAddToCart(req, resp);   break;
            case "remove":   handleRemoveItem(req, resp);  break;
            case "update":   handleUpdateQty(req, resp);   break;
            case "checkout": handleCheckout(req, resp);    break;
            default:
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unknown action: " + action);
        }
    }

    private void handleAddToCart(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {

        try {
            Long productId = Long.parseLong(req.getParameter("productId"));
            int quantity   = Integer.parseInt(req.getParameter("quantity"));

            Product product = productCatalog.getProductById(productId);
            if (product == null) {
                resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Product not found");
                return;
            }

            UserCartSessionBean cart = getOrCreateCart(req.getSession());
            cart.addItem(product.getId(), product.getSku(),
                         product.getName(), quantity, product.getPrice());

            LOG.info("Added to cart: " + product.getSku() + " x" + quantity);
            resp.sendRedirect(req.getContextPath() + "/cart");

        } catch (NumberFormatException e) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid product ID or quantity");
        }
    }

    private void handleRemoveItem(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {

        try {
            Long productId = Long.parseLong(req.getParameter("productId"));
            UserCartSessionBean cart = getOrCreateCart(req.getSession());
            cart.removeItem(productId);
            resp.sendRedirect(req.getContextPath() + "/cart");
        } catch (NumberFormatException e) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid product ID");
        }
    }

    private void handleUpdateQty(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {

        try {
            Long productId = Long.parseLong(req.getParameter("productId"));
            int newQty     = Integer.parseInt(req.getParameter("quantity"));
            UserCartSessionBean cart = getOrCreateCart(req.getSession());
            cart.updateQuantity(productId, newQty);
            resp.sendRedirect(req.getContextPath() + "/cart");
        } catch (NumberFormatException e) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid parameters");
        }
    }

    private void handleCheckout(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        HttpSession session        = req.getSession();
        UserCartSessionBean cart      = getOrCreateCart(session);

        if (cart.isEmpty()) {
            req.setAttribute("error", "Your cart is empty.");
            req.getRequestDispatcher("/pages/cart.jsp").forward(req, resp);
            return;
        }

        String email   = req.getParameter("email");
        String address = req.getParameter("shippingAddress");

        try {
            cart.setCustomerId(getCustomerId(session));
            Order order = cart.buildOrder(email, address);
            Order placed = orderService.placeOrder(order);

            // Wait up to 3 seconds for the async email confirmation
            Future<String> emailFuture = orderService.sendConfirmationEmailAsync(placed);
            try {
                String emailResult = emailFuture.get(3, TimeUnit.SECONDS);
                LOG.info("Async email result: " + emailResult);
            } catch (TimeoutException te) {
                LOG.warning("Email confirmation timed out for order " + placed.getId());
            } catch (ExecutionException | InterruptedException ex) {
                LOG.log(Level.WARNING, "Email confirmation failed", ex);
                Thread.currentThread().interrupt();
            }

            // @Remove the stateful bean and clear the session attribute
            cart.checkout();
            session.removeAttribute(CART_SESSION_KEY);

            req.setAttribute("order", placed);
            req.getRequestDispatcher("/pages/order-confirmation.jsp").forward(req, resp);

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Checkout failed", e);
            req.setAttribute("error", "Checkout failed: " + e.getMessage());
            req.getRequestDispatcher("/pages/cart.jsp").forward(req, resp);
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * Returns the cart bean from the session, creating a new one if absent.
     * In a CDI app this would be @SessionScoped + @Inject.
     */
    private UserCartSessionBean getOrCreateCart(HttpSession session) {
        UserCartSessionBean cart = (UserCartSessionBean) session.getAttribute(CART_SESSION_KEY);
        if (cart == null) {
            try {
                // CORRECTED: JNDI utilities live in the Java SE core standard javax.naming space
                javax.naming.InitialContext ctx = new javax.naming.InitialContext();
                cart = (UserCartSessionBean) ctx.lookup("java:module/UserCartSessionBean");
                session.setAttribute(CART_SESSION_KEY, cart);
            } catch (javax.naming.NamingException e) {
                LOG.log(java.util.logging.Level.SEVERE, "EJB lookup failed for UserCartSessionBean", e);
                throw new RuntimeException("EJB container failed to instantiate stateful session component", e);
            }
        }
        return cart;
    }

    private String getCustomerId(HttpSession session) {
        String id = (String) session.getAttribute("customerId");
        return id != null ? id : "guest-" + session.getId().substring(0, 8);
    }
}
