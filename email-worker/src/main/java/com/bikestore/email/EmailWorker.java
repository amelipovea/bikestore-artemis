package com.bikestore.email;

import java.time.Instant;

import jakarta.jms.JMSException;
import jakarta.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import com.bikestore.contracts.PaymentResult;

import tools.jackson.databind.ObjectMapper;

@Component
public class EmailWorker {

    private static final Logger log =
        LoggerFactory.getLogger(EmailWorker.class);

    private final ObjectMapper mapper;

    public EmailWorker(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @JmsListener(destination = "bikestore.paid")
    public void recibir(TextMessage mensaje)
            throws JMSException {

        PaymentResult resultado = mapper.readValue(
            mensaje.getText(),
            PaymentResult.class
        );

        if (!"PAID".equals(resultado.paymentStatus())) {

            log.warn(
                "pedidoId={} timestamp={} thread={} evento=EMAIL_SKIPPED paymentStatus={}",
                resultado.pedidoId(),
                Instant.now(),
                Thread.currentThread().getName(),
                resultado.paymentStatus()
            );

            return;
        }

        log.info(
            "pedidoId={} timestamp={} thread={} evento=EMAIL_SENT destinatario={} modelo={}",
            resultado.pedidoId(),
            Instant.now(),
            Thread.currentThread().getName(),
            resultado.customerEmail(),
            resultado.bikeModel()
        );
    }
}