package cl.duoc.dsy1107.pedidosapi.messaging;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class Consumers {

    @RabbitListener(queues = RabbitMQConfig.NOTIFICACIONES)
    public void notificar(String eventoJson) {
        System.out.println("[NOTIFICACIÓN] " + eventoJson);
    }

    @RabbitListener(queues = RabbitMQConfig.COCINA)
    public void imprimirTicket(String eventoJson) {
        System.out.println("[COCINA] " + eventoJson);
    }

    @RabbitListener(queues = RabbitMQConfig.DESPACHO)
    public void notificarDespacho(String eventoJson) {
        System.out.println("[DESPACHO] " + eventoJson);
    }
}