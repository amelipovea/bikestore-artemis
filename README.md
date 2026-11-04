# BikeStore Async — ActiveMQ Artemis

Proyecto académico de Diseño y Arquitectura de Software.

## Objetivo

Desarrollar una aplicación de pedidos asíncronos utilizando Java, Spring Boot y Apache ActiveMQ Artemis.

## Arquitectura

El proyecto utiliza Maven multimódulo y separa tres aplicaciones:

- `bikestore-api`: recepción y publicación de pedidos.
- `payment-worker`: procesamiento de pagos simulados.
- `email-worker`: confirmación de pedidos aprobados.

También contiene el módulo `contracts`, encargado de definir los modelos compartidos de los mensajes.

## Estado actual

La estructura multimódulo compila correctamente. La comunicación con ActiveMQ Artemis, los consumidores, los reintentos y la Dead-Letter Queue están pendientes de implementación.

## Compilación

Desde la raíz del repositorio:

```powershell
.\mvnw.cmd -DskipTests clean package
```

La compilación genera tres aplicaciones Spring Boot empaquetadas como archivos JAR.