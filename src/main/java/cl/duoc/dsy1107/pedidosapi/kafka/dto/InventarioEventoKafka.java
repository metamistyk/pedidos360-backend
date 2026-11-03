package cl.duoc.dsy1107.pedidosapi.kafka.dto;

/**
 * Evento que se publica en el tópico "inventario.eventos" cuando el stock de
 * un producto cambia (hoy: al aceptar un pedido se descuenta el stock).
 *
 * La KEY del mensaje es el id del producto: todos los movimientos de un mismo
 * producto caen en la misma partición y se procesan en orden.
 */
public record InventarioEventoKafka(
    String eventId,
    String tipo,
    Long productoId,
    String nombre,
    int cantidad,
    int stockRestante,
    Long pedidoId,
    String timestamp
) {}
