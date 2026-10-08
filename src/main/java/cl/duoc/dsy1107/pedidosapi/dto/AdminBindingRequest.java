package cl.duoc.dsy1107.pedidosapi.dto;
 
import jakarta.validation.constraints.NotBlank;
 
public class AdminBindingRequest {
 
    @NotBlank(message = "El nombre del exchange es obligatorio")
    private String exchange;
 
    @NotBlank(message = "El nombre de la cola es obligatorio")
    private String queue;
 
    @NotBlank(message = "La routing key es obligatoria")
    private String routingKey;
 
    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }
 
    public String getQueue() { return queue; }
    public void setQueue(String queue) { this.queue = queue; }
 
    public String getRoutingKey() { return routingKey; }
    public void setRoutingKey(String routingKey) { this.routingKey = routingKey; }
}
 