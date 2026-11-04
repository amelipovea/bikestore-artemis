package com.bikestore.api;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record OrderRequest(
    @Email @NotBlank String customerEmail,
    @NotBlank String bikeModel,
    @Min(1) int quantity
) {}