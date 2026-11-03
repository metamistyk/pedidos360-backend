package cl.duoc.dsy1107.pedidosapi.kafka.dto;

/**
 * Mensaje que se publica en el tópico "pedidos360.errores" cuando un consumidor
 * agota los reintentos (o recibe un evento inválido). Guarda de dónde venía
 * (tópico, partición, offset), la key, el contenido y el motivo del fallo,
 * para poder revisarlo y reprocesarlo a mano.
 */
public record ErrorEventoKafka(
    String topicoOrigen,
    int particionOrigen,
    long offsetOrigen,
    String key,
    String valor,
    String error,
    String timestamp
) {}
