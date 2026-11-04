package com.bikestore.api;

import java.util.Map;

import jakarta.validation.Valid;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> crear(
        @Valid @RequestBody OrderRequest request
    ) {
        String pedidoId = producer.publicar(request);

        return ResponseEntity.accepted().body(
            Map.of(
                "pedidoId", pedidoId,
                "status", "ACCEPTED"
            )
        );
    }
}