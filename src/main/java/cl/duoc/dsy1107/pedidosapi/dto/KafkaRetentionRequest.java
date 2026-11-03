package cl.duoc.dsy1107.pedidosapi.dto;

import jakarta.validation.constraints.Min;

public class KafkaRetentionRequest {

    @Min(value = 1, message = "La retencion (ms) debe ser mayor a 0")
    private long retencionMs;

    public long getRetencionMs() { return retencionMs; }
    public void setRetencionMs(long retencionMs) { this.retencionMs = retencionMs; }
}
