package cl.duoc.dsy1107.pedidosapi.messaging;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String EXCHANGE = "pedidos360.direct.exchange";
    public static final String NOTIFICACIONES = "notificaciones.queue";
    public static final String COCINA = "cocina.queue";
    public static final String DESPACHO = "despacho.queue";

    // ===== Confiabilidad: DLX + reintentos con TTL + DLQ (aplicado a COCINA) =====
    public static final String DLX = "pedidos360.dlx";
    public static final String COCINA_RETRY_ROUTING_KEY = "cocina.retry";
    public static final String COCINA_RETRY_QUEUE = "cocina.retry.queue";
    public static final String COCINA_DLQ = "cocina.dlq";
    private static final int RETRY_TTL_MS = 5000;

    @Bean
    DirectExchange pedidosExchange() {
        return new DirectExchange(EXCHANGE, true, false);
    }

    @Bean
    Queue notificacionesQueue() {
        return new Queue(NOTIFICACIONES, true);
    }

    // Antes era "new Queue(COCINA, true)". Ahora declara a dónde va un mensaje
    // cuando el consumer lo rechaza (NACK): a la DLX, con routing key "cocina.retry".
    @Bean
    Queue cocinaQueue() {
        return QueueBuilder.durable(COCINA)
            .withArgument("x-dead-letter-exchange", DLX)
            .withArgument("x-dead-letter-routing-key", COCINA_RETRY_ROUTING_KEY)
            .build();
    }

    @Bean
    Queue despachoQueue() {
        return new Queue(DESPACHO, true);
    }

    @Bean
    Binding notificaCreado(DirectExchange pedidosExchange, Queue notificacionesQueue) {
        return BindingBuilder.bind(notificacionesQueue).to(pedidosExchange).with("pedido.creado");
    }

    @Bean
    Binding notificaAceptado(DirectExchange pedidosExchange, Queue notificacionesQueue) {
        return BindingBuilder.bind(notificacionesQueue).to(pedidosExchange).with("pedido.aceptado");
    }

    @Bean
    Binding cocinaAceptado(DirectExchange pedidosExchange, Queue cocinaQueue) {
        return BindingBuilder.bind(cocinaQueue).to(pedidosExchange).with("pedido.aceptado");
    }

    @Bean
    Binding notificaDespachado(DirectExchange pedidosExchange, Queue notificacionesQueue) {
        return BindingBuilder.bind(notificacionesQueue).to(pedidosExchange).with("pedido.despachado");
    }

    @Bean
    Binding despachoDespachado(DirectExchange pedidosExchange, Queue despachoQueue) {
        return BindingBuilder.bind(despachoQueue).to(pedidosExchange).with("pedido.despachado");
    }

    // ===== Piezas de confiabilidad: DLX, cola de reintento (con TTL) y DLQ final =====

    @Bean
    DirectExchange dlx() {
        return new DirectExchange(DLX, true, false);
    }

    // Cola "de espera": nadie la consume. Cuando un mensaje lleva 5s aquí (TTL),
    // RabbitMQ lo expulsa automáticamente y, por su propia dead-letter-config,
    // lo devuelve al exchange original con la routing key original -> vuelve a cocina.queue.
    @Bean
    Queue cocinaRetryQueue() {
        return QueueBuilder.durable(COCINA_RETRY_QUEUE)
            .withArgument("x-message-ttl", RETRY_TTL_MS)
            .withArgument("x-dead-letter-exchange", EXCHANGE)
            .withArgument("x-dead-letter-routing-key", "pedido.aceptado")
            .build();
    }

    @Bean
    Binding cocinaRetryBinding(DirectExchange dlx, Queue cocinaRetryQueue) {
        return BindingBuilder.bind(cocinaRetryQueue).to(dlx).with(COCINA_RETRY_ROUTING_KEY);
    }

    // Cola final: aquí quedan los mensajes que fallaron demasiadas veces.
    // No tiene reintentos ni TTL — es solo para inspección manual (management UI).
    @Bean
    Queue cocinaDlq() {
        return new Queue(COCINA_DLQ, true);
    }
}