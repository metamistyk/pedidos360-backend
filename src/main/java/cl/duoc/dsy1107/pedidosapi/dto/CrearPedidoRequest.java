package cl.duoc.dsy1107.pedidosapi.dto;

import java.util.List;

public class CrearPedidoRequest {
    private List<ItemRequest> items;

    public List<ItemRequest> getItems() { return items; }
    public void setItems(List<ItemRequest> items) { this.items = items; }
}