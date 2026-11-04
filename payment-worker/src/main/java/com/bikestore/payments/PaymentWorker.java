package com.bikestore.payments;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

import jakarta.jms.JMSException;
import jakarta.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import com.bikestore.contracts.OrderMessage;
import tools.jackson.databind.ObjectMapper;

@Component
public class PaymentWorker {

    private static final Logger log =
        LoggerFactory.getLogger(PaymentWorker.class);

    private final ObjectMapper mapper;
    private final boolean forceFailure;

    public PaymentWorker(
        ObjectMapper mapper,
        @Value("${bikestore.payment.force-failure:false}")
        boolean forceFailure
    ) {
        this.mapper = mapper;
        this.forceFailure = forceFailure;
    }

    @JmsListener(destination = "bikestore.orders")
    public void recibir(TextMessage mensaje) throws JMSException {

        String json = mensaje.getText();
        OrderMessage pedido =
            mapper.readValue(json, OrderMessage.class);

        int intento = mensaje.getIntProperty(
            "JMSXDeliveryCount"
        );

        log.info(
            "pedidoId={} timestamp={} thread={} evento=PAYMENT_ATTEMPT intento={}",
            pedido.pedidoId(),
            Instant.now(),
            Thread.currentThread().getName(),
            intento
        );

        boolean fallo = forceFailure ||
            ThreadLocalRandom.current().nextBoolean();

        if (fallo) {

            log.warn(
                "pedidoId={} timestamp={} thread={} evento=PAYMENT_FAILED intento={}",
                pedido.pedidoId(),
                Instant.now(),
                Thread.currentThread().getName(),
                intento
            );

            if (intento >= 4) {
                log.error(
                    "pedidoId={} timestamp={} thread={} evento=RETRIES_EXHAUSTED",
                    pedido.pedidoId(),
                    Instant.now(),
                    Thread.currentThread().getName()
                );
            }

            throw new IllegalStateException(
                "Pago simulado rechazado"
            );
        }

        log.info(
            "pedidoId={} timestamp={} thread={} evento=PAYMENT_PAID intento={}",
            pedido.pedidoId(),
            Instant.now(),
            Thread.currentThread().getName(),
            intento
        );
    }
}