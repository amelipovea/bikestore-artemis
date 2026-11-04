package com.bikestore.contracts;

public record PaymentResult(
    String pedidoId,
    String customerEmail,
    String bikeModel,
    int quantity,
    String paymentStatus,
    String paidAt
) {}