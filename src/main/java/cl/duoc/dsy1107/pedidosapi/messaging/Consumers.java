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

    // Máximo de veces que se reintenta un mensaje antes de mandarlo a su DLQ.
    private static final int MAX_REINTENTOS = 3;

    private final RabbitTemplate rabbitTemplate;

    public Consumers(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    // Con ackMode=manual (ver application.properties), TODO listener debe
    // confirmar el mensaje a mano con channel.basicAck — si no, el broker
    // nunca lo da por recibido y se reencola sin fin al reiniciar el consumer.

    @RabbitListener(queues = RabbitMQConfig.NOTIFICACIONES)
    public void notificar(Message mensaje, Channel channel,
                           @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        procesarConReintento(
            mensaje, channel, tag,
            "NOTIFICACIÓN",
            RabbitMQConfig.NOTIFICACIONES_DLQ
        );
    }

    @RabbitListener(queues = RabbitMQConfig.COCINA)
    public void imprimirTicket(Message mensaje, Channel channel,
                                @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        procesarConReintento(
            mensaje, channel, tag,
            "COCINA",
            RabbitMQConfig.COCINA_DLQ
        );
    }

    @RabbitListener(queues = RabbitMQConfig.DESPACHO)
    public void notificarDespacho(Message mensaje, Channel channel,
                                    @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        procesarConReintento(
            mensaje, channel, tag,
            "DESPACHO",
            RabbitMQConfig.DESPACHO_DLQ
        );
    }

    // Llega aquí TODO evento cuya routing key empiece con "pedido." (creado,
    // aceptado, despachado, cancelado), gracias al wildcard del Topic Exchange
    // — sin necesitar una binding exacta por cada uno, como en el Direct Exchange.
    @RabbitListener(queues = RabbitMQConfig.AUDITORIA)
    public void auditar(String eventoJson, Channel channel,
                         @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        System.out.println("[AUDITORÍA] " + eventoJson);
        channel.basicAck(tag, false);
    }

    // Lógica común de ACK/NACK + reintento + DLQ, reutilizada por los 3
    // consumers de negocio (notificaciones, cocina, despacho).
    private void procesarConReintento(Message mensaje, Channel channel, long tag,
                                       String etiqueta, String dlq) throws IOException {
        String eventoJson = new String(mensaje.getBody());
        try {
            // --- Simulación de fallo para probar el flujo de reintentos/DLQ ---
            // Descomenta esta línea en el consumer que quieras probar:
            // if (true) throw new RuntimeException(etiqueta + " no responde");

            System.out.println("[" + etiqueta + "] " + eventoJson);
            channel.basicAck(tag, false);

        } catch (Exception ex) {
            int reintentos = contarReintentos(mensaje);
            System.out.println("[" + etiqueta + "][ERROR] intento " + (reintentos + 1) + " de " + MAX_REINTENTOS
                + " — " + ex.getMessage());

            if (reintentos < MAX_REINTENTOS) {
                // NACK sin reencolar en la misma cola: por la configuración de
                // la cola, RabbitMQ lo manda a la DLX -> su cola de retry,
                // espera 5s (TTL) y lo devuelve solo a la cola original.
                channel.basicNack(tag, false, false);
            } else {
                System.out.println("[" + etiqueta + "][DLQ] se superaron los reintentos, moviendo a " + dlq);
                rabbitTemplate.convertAndSend("", dlq, eventoJson);
                channel.basicAck(tag, false); // saca el mensaje de la cola original definitivamente
            }
        }
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