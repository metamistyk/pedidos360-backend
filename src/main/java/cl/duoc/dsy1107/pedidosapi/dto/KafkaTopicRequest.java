package cl.duoc.dsy1107.pedidosapi.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class KafkaTopicRequest {

    @NotBlank(message = "El nombre del topico no puede estar vacio")
    private String nombre;

    @Min(value = 1, message = "Las particiones deben ser al menos 1")
    private int particiones = 3;

    @Min(value = 1, message = "El factor de replicacion debe ser al menos 1")
    private int factorReplicacion = 3;

    @Min(value = 1, message = "La retencion (ms) debe ser mayor a 0")
    private long retencionMs = 604800000L;

    public String getNombre() { return nombre; }
    public void setNombre(String nombre) { this.nombre = nombre; }

    public int getParticiones() { return particiones; }
    public void setParticiones(int particiones) { this.particiones = particiones; }

    public int getFactorReplicacion() { return factorReplicacion; }
    public void setFactorReplicacion(int factorReplicacion) { this.factorReplicacion = factorReplicacion; }

    public long getRetencionMs() { return retencionMs; }
    public void setRetencionMs(long retencionMs) { this.retencionMs = retencionMs; }
}
