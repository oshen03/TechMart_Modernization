<%@ page contentType="text/html;charset=UTF-8" language="java" isErrorPage="true" %>
<%@ page import="jakarta.servlet.RequestDispatcher" %>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Error — TechMart</title>
    <link rel="stylesheet" href="${pageContext.request.contextPath}/css/style.css">
</head>
<body>
<jsp:include page="/pages/header.jsp"/>

<main class="container">
    <%
        Integer statusCode = (Integer) request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
        String message = (String) request.getAttribute(RequestDispatcher.ERROR_MESSAGE);
        if (statusCode == null) statusCode = 500;
        if (message == null || message.isEmpty()) message = "Something went wrong.";
    %>
    <div class="alert alert-error">
        <h2>Error <%= statusCode %></h2>
        <p><%= message %></p>
    </div>

    <a href="${pageContext.request.contextPath}/" class="btn btn-primary">Back to Home</a>
</main>

<jsp:include page="/pages/footer.jsp"/>
</body>
</html>