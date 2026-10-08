package cl.duoc.dsy1107.pedidosapi.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
public class PedidoEventoProducer {

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    public PedidoEventoProducer(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
    }

    public void publicar(String routingKey, PedidoEvento evento) {
        try {
            String payload = objectMapper.writeValueAsString(evento);

            // Exchange de negocio (Direct): la routing key exacta decide a qué
            // cola(s) llega — notificaciones.queue, cocina.queue o despacho.queue.
            rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE, routingKey, payload);

            // Exchange de auditoría (Topic): una sola binding con wildcard
            // ("pedido.*") capta cualquier routing key que empiece con "pedido."
            // sin tener que declarar una binding por cada tipo de evento.
            rabbitTemplate.convertAndSend(RabbitMQConfig.TOPIC_EXCHANGE, routingKey, payload);

        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No fue posible serializar el evento", e);
        }
    }
}