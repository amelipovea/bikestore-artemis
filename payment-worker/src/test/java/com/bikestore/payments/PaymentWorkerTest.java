package com.bikestore.payments;

import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class PaymentWorkerTest {

    @Test
    void pagoForzadoAFallarDebeLanzarExcepcion() throws Exception {

        TextMessage mensaje = mock(TextMessage.class);
        Session session = mock(Session.class);

        String json = """
            {
              "pedidoId": "pedido-prueba",
              "customerEmail": "cliente@example.com",
              "bikeModel": "Ruta",
              "quantity": 1,
              "createdAt": "2026-11-03T23:00:00Z"
            }
            """;

        when(mensaje.getText()).thenReturn(json);
        when(mensaje.getIntProperty("JMSXDeliveryCount"))
            .thenReturn(4);

        PaymentWorker worker = new PaymentWorker(
            new ObjectMapper(),
            true
        );

        assertThrows(
            IllegalStateException.class,
            () -> worker.recibir(mensaje, session)
        );

        verifyNoInteractions(session);
    }
}
