package cl.duoc.dsy1107.pedidosapi.dto;

import jakarta.validation.constraints.NotBlank;

public class AdminQueueRequest {

    @NotBlank(message = "El nombre de la cola no puede estar vacio")
    private String nombre;

    private boolean durable = true;

    public String getNombre() { return nombre; }
    public void setNombre(String nombre) { this.nombre = nombre; }

    public boolean isDurable() { return durable; }
    public void setDurable(boolean durable) { this.durable = durable; }
}