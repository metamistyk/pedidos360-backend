package cl.duoc.dsy1107.pedidosapi.messaging;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String EXCHANGE = "pedidos360.direct.exchange";
    public static final String NOTIFICACIONES = "notificaciones.queue";
    public static final String COCINA = "cocina.queue";
    public static final String DESPACHO = "despacho.queue";

    @Bean
    DirectExchange pedidosExchange() {
        return new DirectExchange(EXCHANGE, true, false);
    }

    @Bean
    Queue notificacionesQueue() {
        return new Queue(NOTIFICACIONES, true);
    }

    @Bean
    Queue cocinaQueue() {
        return new Queue(COCINA, true);
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
}