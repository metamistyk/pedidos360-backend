package cl.duoc.dsy1107.pedidosapi.messaging;

import java.time.LocalDateTime;

public record PedidoEvento(
    Long pedidoId,
    String estado,
    String clienteId,
    LocalDateTime timestamp
) {}