package cl.duoc.dsy1107.pedidosapi.messaging;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Component
public class Consumers {

    // Máximo de veces que se reintenta "cocina" antes de mandarlo a la DLQ.
    private static final int MAX_REINTENTOS = 3;

    private final RabbitTemplate rabbitTemplate;

    public Consumers(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    // Con ackMode=manual (ver application.properties), TODO listener debe
    // confirmar el mensaje a mano con channel.basicAck — si no, el broker
    // nunca lo da por recibido y se reencola sin fin al reiniciar el consumer.

    @RabbitListener(queues = RabbitMQConfig.NOTIFICACIONES)
    public void notificar(String eventoJson, Channel channel,
                           @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.out.println("[NOTIFICACIÓN] " + eventoJson);
        channel.basicAck(tag, false);
    }

    @RabbitListener(queues = RabbitMQConfig.COCINA)
    public void imprimirTicket(Message mensaje, Channel channel,
                                @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        String eventoJson = new String(mensaje.getBody());
        try {
            // --- Simulación de fallo para probar el flujo de reintentos/DLQ ---
            // Descomenta esta línea, crea un pedido y acéptalo para ver cocina.queue
            // fallar 3 veces (una cada 5s) y terminar en cocina.dlq:
            // if (true) throw new RuntimeException("Impresora de cocina no responde");

            System.out.println("[COCINA] " + eventoJson);
            channel.basicAck(tag, false);

        } catch (Exception ex) {
            int reintentos = contarReintentos(mensaje);
            System.out.println("[COCINA][ERROR] intento " + (reintentos + 1) + " de " + MAX_REINTENTOS
                + " — " + ex.getMessage());

            if (reintentos < MAX_REINTENTOS) {
                // NACK sin reencolar en la misma cola: por la configuración de
                // cocina.queue, RabbitMQ lo manda a la DLX -> cocina.retry.queue,
                // espera 5s (TTL) y lo devuelve solo a cocina.queue.
                channel.basicNack(tag, false, false);
            } else {
                System.out.println("[COCINA][DLQ] se superaron los reintentos, moviendo a " + RabbitMQConfig.COCINA_DLQ);
                rabbitTemplate.convertAndSend("", RabbitMQConfig.COCINA_DLQ, eventoJson);
                channel.basicAck(tag, false); // saca el mensaje de cocina.queue definitivamente
            }
        }
    }

    @RabbitListener(queues = RabbitMQConfig.DESPACHO)
    public void notificarDespacho(String eventoJson, Channel channel,
                                    @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.out.println("[DESPACHO] " + eventoJson);
        channel.basicAck(tag, false);
    }

    // RabbitMQ agrega el header "x-death" cada vez que un mensaje pasa por una
    // dead-letter-exchange. Ahí viaja un contador "count" por cada ciclo.
    @SuppressWarnings("unchecked")
    private int contarReintentos(Message mensaje) {
        List<Map<String, ?>> xDeath = mensaje.getMessageProperties().getXDeathHeader();
        if (xDeath == null || xDeath.isEmpty()) {
            return 0;
        }
        Object count = xDeath.get(0).get("count");
        return count == null ? 0 : ((Number) count).intValue();
    }
}