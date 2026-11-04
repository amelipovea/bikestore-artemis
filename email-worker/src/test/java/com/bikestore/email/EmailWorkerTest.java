package com.bikestore.email;

import jakarta.jms.TextMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import tools.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(OutputCaptureExtension.class)
class EmailWorkerTest {

    @Test
    void pagoAprobadoDebeRegistrarCorreo(
        CapturedOutput salida
    ) throws Exception {

        TextMessage mensaje = mock(TextMessage.class);

        when(mensaje.getText()).thenReturn("""
            {
              "pedidoId": "pedido-pagado",
              "customerEmail": "cliente@example.com",
              "bikeModel": "Ruta",
              "quantity": 1,
              "paymentStatus": "PAID",
              "paidAt": "2026-11-03T23:00:00Z"
            }
            """);

        EmailWorker worker = new EmailWorker(
            new ObjectMapper()
        );

        worker.recibir(mensaje);

        assertTrue(
            salida.getAll().contains("evento=EMAIL_SENT")
        );

        assertTrue(
            salida.getAll().contains("pedido-pagado")
        );
    }

    @Test
    void pagoRechazadoNoDebeRegistrarCorreo(
        CapturedOutput salida
    ) throws Exception {

        TextMessage mensaje = mock(TextMessage.class);

        when(mensaje.getText()).thenReturn("""
            {
              "pedidoId": "pedido-rechazado",
              "customerEmail": "cliente@example.com",
              "bikeModel": "Ruta",
              "quantity": 1,
              "paymentStatus": "FAILED",
              "paidAt": ""
            }
            """);

        EmailWorker worker = new EmailWorker(
            new ObjectMapper()
        );

        worker.recibir(mensaje);

        assertTrue(
            salida.getAll().contains("evento=EMAIL_SKIPPED")
        );

        assertFalse(
            salida.getAll().contains("evento=EMAIL_SENT")
        );
    }
}
