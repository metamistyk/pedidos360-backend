package cl.duoc.dsy1107.pedidosapi.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public class AdminExchangeRequest {

    @NotBlank(message = "El nombre del exchange no puede estar vacio")
    private String nombre;

    // Solo se permiten estos 3 tipos: el proyecto no usa Headers exchange.
    @NotBlank(message = "El tipo de exchange es obligatorio")
    @Pattern(regexp = "direct|topic|fanout", message = "El tipo debe ser 'direct', 'topic' o 'fanout'")
    private String tipo;

    private boolean durable = true;

    public String getNombre() { return nombre; }
    public void setNombre(String nombre) { this.nombre = nombre; }

    public String getTipo() { return tipo; }
    public void setTipo(String tipo) { this.tipo = tipo; }

    public boolean isDurable() { return durable; }
    public void setDurable(boolean durable) { this.durable = durable; }
}