package cl.duoc.dsy1107.pedidosapi.kafka.dto;

import java.math.BigDecimal;

/** Una línea del pedido dentro del evento (producto, cantidad y precio al momento de comprar). */
public record ItemEventoKafka(
    Long productoId,
    String nombre,
    int cantidad,
    BigDecimal precioUnitario
) {}
