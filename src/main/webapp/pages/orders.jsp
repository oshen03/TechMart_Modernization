<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib prefix="c" uri="jakarta.tags.core" %>
<%@ taglib prefix="fmt" uri="jakarta.tags.fmt" %>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>My Orders — TechMart</title>
    <link rel="stylesheet" href="${pageContext.request.contextPath}/css/style.css">
</head>
<body>
<jsp:include page="/pages/header.jsp"/>

<main class="container">
    <h2>My Orders</h2>

    <c:choose>
        <c:when test="${not empty orders}">
            <table class="order-table">
                <thead>
                <tr>
                    <th>Order ID</th>
                    <th>Status</th>
                    <th>Total</th>
                    <th>Placed At</th>
                    <th>Shipping Address</th>
                </tr>
                </thead>
                <tbody>
                <c:forEach var="order" items="${orders}">
                    <tr>
                        <td>#${order.id}</td>
                        <td class="status-badge">${order.status}</td>
                        <td>$<fmt:formatNumber value="${order.totalAmount}" pattern="#,##0.00"/></td>
                        <td>${order.placedAt}</td>
                        <td>${order.shippingAddress}</td>
                    </tr>
                </c:forEach>
                </tbody>
            </table>
        </c:when>
        <c:otherwise>
            <div class="empty-state">
                <p>You haven't placed any orders yet.</p>
                <a href="${pageContext.request.contextPath}/products" class="btn btn-primary">Browse Products</a>
            </div>
        </c:otherwise>
    </c:choose>
</main>

<jsp:include page="/pages/footer.jsp"/>
</body>
</html>