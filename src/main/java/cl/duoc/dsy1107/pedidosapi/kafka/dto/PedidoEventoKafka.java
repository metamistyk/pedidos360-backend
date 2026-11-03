package cl.duoc.dsy1107.pedidosapi.kafka.dto;

import java.math.BigDecimal;
import java.util.List;

/**
 * Evento de dominio que se publica en el tópico "pedidos.eventos" cada vez que
 * un pedido cambia de estado.
 *
 *  - eventId:   identificador único del evento (permite detectar duplicados).
 *  - tipo:      PEDIDO_CREADO, PEDIDO_ACEPTADO, PEDIDO_EN_PREPARACION,
 *               PEDIDO_DESPACHADO, PEDIDO_ENTREGADO o PEDIDO_CANCELADO.
 *  - timestamp: fecha y hora en formato ISO-8601 (texto, legible en Kafka UI).
 *
 * La KEY del mensaje en Kafka es el id del pedido: todos los eventos de un
 * mismo pedido caen en la misma partición y por tanto se leen EN ORDEN.
 */
public record PedidoEventoKafka(
    String eventId,
    String tipo,
    Long pedidoId,
    String estado,
    String clienteId,
    BigDecimal total,
    List<ItemEventoKafka> items,
    String timestamp
) {}
