package cl.duoc.dsy1107.pedidosapi.dto;

import java.math.BigDecimal;

public class ProductoRequest {
    private String nombre;
    private BigDecimal precio;
    private Integer stock;

    public String getNombre() { return nombre; }
    public void setNombre(String nombre) { this.nombre = nombre; }

    public BigDecimal getPrecio() { return precio; }
    public void setPrecio(BigDecimal precio) { this.precio = precio; }

    public Integer getStock() { return stock; }
    public void setStock(Integer stock) { this.stock = stock; }
}