package com.bikestore.api;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

import com.bikestore.contracts.OrderMessage;
import tools.jackson.databind.ObjectMapper;

@Service
public class OrderProducer {

    private static final Logger log =
        LoggerFactory.getLogger(OrderProducer.class);

    private final JmsTemplate jmsTemplate;
    private final ObjectMapper mapper;

    public OrderProducer(
        JmsTemplate jmsTemplate,
        ObjectMapper mapper
    ) {
        this.jmsTemplate = jmsTemplate;
        this.mapper = mapper;
    }

    public String publicar(OrderRequest request) {

        String pedidoId = UUID.randomUUID().toString();

        OrderMessage pedido = new OrderMessage(
            pedidoId,
            request.customerEmail(),
            request.bikeModel(),
            request.quantity(),
            Instant.now().toString()
        );

        String json = mapper.writeValueAsString(pedido);

        jmsTemplate.convertAndSend("bikestore.orders", json);

        log.info(
            "pedidoId={} timestamp={} thread={} evento=ORDER_PUBLISHED",
            pedidoId,
            Instant.now(),
            Thread.currentThread().getName()
        );

        return pedidoId;
    }
}