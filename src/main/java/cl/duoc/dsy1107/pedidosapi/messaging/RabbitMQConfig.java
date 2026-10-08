package cl.duoc.dsy1107.pedidosapi.messaging;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String EXCHANGE = "pedidos360.direct.exchange";
    public static final String NOTIFICACIONES = "notificaciones.queue";
    public static final String COCINA = "cocina.queue";
    public static final String DESPACHO = "despacho.queue";

    // ===== Confiabilidad: DLX compartida + reintentos con TTL + DLQ por cola =====
    public static final String DLX = "pedidos360.dlx";
    private static final int RETRY_TTL_MS = 5000;

    public static final String NOTIFICACIONES_RETRY_ROUTING_KEY = "notificaciones.retry";
    public static final String NOTIFICACIONES_RETRY_QUEUE = "notificaciones.retry.queue";
    public static final String NOTIFICACIONES_DLQ = "notificaciones.dlq";

    public static final String COCINA_RETRY_ROUTING_KEY = "cocina.retry";
    public static final String COCINA_RETRY_QUEUE = "cocina.retry.queue";
    public static final String COCINA_DLQ = "cocina.dlq";

    public static final String DESPACHO_RETRY_ROUTING_KEY = "despacho.retry";
    public static final String DESPACHO_RETRY_QUEUE = "despacho.retry.queue";
    public static final String DESPACHO_DLQ = "despacho.dlq";

    // ===== Topic Exchange: una sola cola de auditoría para TODOS los eventos =====
    public static final String TOPIC_EXCHANGE = "pedidos360.topic.exchange";
    public static final String AUDITORIA = "auditoria.queue";
    // "pedido.*" matchea pedido.creado / pedido.aceptado / pedido.despachado / pedido.cancelado
    // (un solo segmento después de "pedido."). NO matchearía algo como
    // "pedido.item.agregado" (dos segmentos) — para eso haría falta "pedido.#".
    public static final String AUDITORIA_PATTERN = "pedido.*";

    @Bean
    DirectExchange pedidosExchange() {
        return new DirectExchange(EXCHANGE, true, false);
    }

    // Cada cola de negocio declara, con argumentos, a dónde va un mensaje
    // cuando el consumer lo rechaza (NACK): a la DLX, con una routing key de
    // retry propia de su dominio (así cada una cae en SU cola de espera).

    @Bean
    Queue notificacionesQueue() {
        return QueueBuilder.durable(NOTIFICACIONES)
            .withArgument("x-dead-letter-exchange", DLX)
            .withArgument("x-dead-letter-routing-key", NOTIFICACIONES_RETRY_ROUTING_KEY)
            .build();
    }

    @Bean
    Queue cocinaQueue() {
        return QueueBuilder.durable(COCINA)
            .withArgument("x-dead-letter-exchange", DLX)
            .withArgument("x-dead-letter-routing-key", COCINA_RETRY_ROUTING_KEY)
            .build();
    }

    @Bean
    Queue despachoQueue() {
        return QueueBuilder.durable(DESPACHO)
            .withArgument("x-dead-letter-exchange", DLX)
            .withArgument("x-dead-letter-routing-key", DESPACHO_RETRY_ROUTING_KEY)
            .build();
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

    // ===== DLX compartida =====

    @Bean
    DirectExchange dlx() {
        return new DirectExchange(DLX, true, false);
    }

    // ===== Colas de espera (retry, con TTL) =====
    // Nadie las consume. Cuando un mensaje lleva 5s ahí, RabbitMQ lo expulsa
    // automáticamente (TTL) y, por su propia dead-letter-config, lo devuelve
    // al exchange original. Como NO fijamos "x-dead-letter-routing-key" aquí,
    // RabbitMQ reutiliza la routing key ORIGINAL del mensaje (necesario para
    // notificaciones.queue, que recibe 3 routing keys distintas).

    @Bean
    Queue notificacionesRetryQueue() {
        return QueueBuilder.durable(NOTIFICACIONES_RETRY_QUEUE)
            .withArgument("x-message-ttl", RETRY_TTL_MS)
            .withArgument("x-dead-letter-exchange", EXCHANGE)
            .build();
    }

    @Bean
    Queue cocinaRetryQueue() {
        return QueueBuilder.durable(COCINA_RETRY_QUEUE)
            .withArgument("x-message-ttl", RETRY_TTL_MS)
            .withArgument("x-dead-letter-exchange", EXCHANGE)
            .build();
    }

    @Bean
    Queue despachoRetryQueue() {
        return QueueBuilder.durable(DESPACHO_RETRY_QUEUE)
            .withArgument("x-message-ttl", RETRY_TTL_MS)
            .withArgument("x-dead-letter-exchange", EXCHANGE)
            .build();
    }

    @Bean
    Binding notificacionesRetryBinding(DirectExchange dlx, Queue notificacionesRetryQueue) {
        return BindingBuilder.bind(notificacionesRetryQueue).to(dlx).with(NOTIFICACIONES_RETRY_ROUTING_KEY);
    }

    @Bean
    Binding cocinaRetryBinding(DirectExchange dlx, Queue cocinaRetryQueue) {
        return BindingBuilder.bind(cocinaRetryQueue).to(dlx).with(COCINA_RETRY_ROUTING_KEY);
    }

    @Bean
    Binding despachoRetryBinding(DirectExchange dlx, Queue despachoRetryQueue) {
        return BindingBuilder.bind(despachoRetryQueue).to(dlx).with(DESPACHO_RETRY_ROUTING_KEY);
    }

    // ===== Colas finales (DLQ) =====
    // Aquí quedan los mensajes que fallaron demasiadas veces. No tienen
    // reintentos ni TTL — solo inspección manual (management UI).

    @Bean
    Queue notificacionesDlq() {
        return new Queue(NOTIFICACIONES_DLQ, true);
    }

    @Bean
    Queue cocinaDlq() {
        return new Queue(COCINA_DLQ, true);
    }

    @Bean
    Queue despachoDlq() {
        return new Queue(DESPACHO_DLQ, true);
    }

    // ===== Topic Exchange: demuestra routing con wildcard =====

    @Bean
    TopicExchange auditoriaExchange() {
        return new TopicExchange(TOPIC_EXCHANGE, true, false);
    }

    @Bean
    Queue auditoriaQueue() {
        return new Queue(AUDITORIA, true);
    }

    @Bean
    Binding auditoriaBinding(TopicExchange auditoriaExchange, Queue auditoriaQueue) {
        return BindingBuilder.bind(auditoriaQueue).to(auditoriaExchange).with(AUDITORIA_PATTERN);
    }
}