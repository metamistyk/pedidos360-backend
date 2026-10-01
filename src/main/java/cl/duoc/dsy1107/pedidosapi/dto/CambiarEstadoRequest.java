package cl.duoc.dsy1107.pedidosapi.dto;

import cl.duoc.dsy1107.pedidosapi.domain.EstadoPedido;

public class CambiarEstadoRequest {
    private EstadoPedido nuevoEstado;

    public EstadoPedido getNuevoEstado() { return nuevoEstado; }
    public void setNuevoEstado(EstadoPedido nuevoEstado) { this.nuevoEstado = nuevoEstado; }
}