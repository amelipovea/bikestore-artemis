package com.bikestore.contracts;

public record OrderMessage(
    String pedidoId,
    String customerEmail,
    String bikeModel,
    int quantity,
    String createdAt
) {}
