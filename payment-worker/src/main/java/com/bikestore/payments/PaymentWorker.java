package com.bikestore.payments;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import com.bikestore.contracts.OrderMessage;
import com.bikestore.contracts.PaymentResult;

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
    public void recibir(
        TextMessage mensaje,
        Session session
    ) throws JMSException {

        OrderMessage pedido = mapper.readValue(
            mensaje.getText(),
            OrderMessage.class
        );

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

        PaymentResult resultado = new PaymentResult(
            pedido.pedidoId(),
            pedido.customerEmail(),
            pedido.bikeModel(),
            pedido.quantity(),
            "PAID",
            Instant.now().toString()
        );

        String json = mapper.writeValueAsString(resultado);

        TextMessage evento = session.createTextMessage(json);
        evento.setJMSCorrelationID(pedido.pedidoId());

        MessageProducer productor = session.createProducer(
            session.createQueue("bikestore.paid")
        );

        try {
            productor.send(
                evento,
                DeliveryMode.PERSISTENT,
                4,
                0
            );
        } finally {
            productor.close();
        }

        log.info(
            "pedidoId={} timestamp={} thread={} evento=PAYMENT_PAID intento={}",
            pedido.pedidoId(),
            Instant.now(),
            Thread.currentThread().getName(),
            intento
        );

        log.info(
            "pedidoId={} timestamp={} thread={} evento=PAYMENT_EVENT_PUBLISHED destino=bikestore.paid",
            pedido.pedidoId(),
            Instant.now(),
            Thread.currentThread().getName()
        );
    }
}