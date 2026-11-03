package cl.duoc.dsy1107.pedidosapi.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import cl.duoc.dsy1107.pedidosapi.kafka.dto.PedidoEventoKafka;

/**
 * Consumer group "pedidos360-notificaciones" sobre pedidos.eventos.
 *
 * Por cada evento de pedido arma el aviso que recibiría el cliente (aquí solo
 * se imprime; en un sistema real iría un correo o push).
 *
 * concurrency = 3: se crean 3 consumidores dentro del mismo grupo. Kafka les
 * reparte las 3 particiones (uno por partición), por eso en Kafka UI → Consumers
 * se ven 3 miembros. Los eventos de un mismo pedido llegan en orden porque
 * comparten key y, por tanto, partición.
 *
 * Para demostrar reintentos y tópico de errores sin tocar código: arrancar el
 * backend con PEDIDOS360_KAFKA_SIMULAR_FALLO=true. Los eventos PEDIDO_ACEPTADO
 * fallarán siempre y, tras los reintentos, irán a "pedidos360.errores".
 */
@Component
public class NotificacionesKafkaConsumer {

    private final boolean simularFallo;

    public NotificacionesKafkaConsumer(@Value("${pedidos360.kafka.simular-fallo:false}") boolean simularFallo) {
        this.simularFallo = simularFallo;
    }

    @KafkaListener(
        topics = KafkaTopics.PEDIDOS_EVENTOS,
        groupId = KafkaGrupos.NOTIFICACIONES,
        concurrency = "3")
    public void consumir(ConsumerRecord<String, PedidoEventoKafka> record) {
        PedidoEventoKafka evento = record.value();
        validar(evento);

        if (simularFallo && "PEDIDO_ACEPTADO".equals(evento.tipo())) {
            throw new IllegalStateException("fallo simulado al notificar el pedido " + evento.pedidoId());
        }

        System.out.println("[KAFKA][NOTIFICACIONES] particion=" + record.partition()
            + " offset=" + record.offset() + " key=" + record.key()
            + " -> " + armarAviso(evento));
    }

    private void validar(PedidoEventoKafka e) {
        if (e == null) {
            throw new EventoInvalidoException("evento vacío");
        }
        if (e.pedidoId() == null) {
            throw new EventoInvalidoException("evento sin pedidoId (eventId=" + e.eventId() + ")");
        }
        if (e.tipo() == null || e.tipo().isBlank()) {
            throw new EventoInvalidoException("evento sin tipo (pedidoId=" + e.pedidoId() + ")");
        }
    }

    private String armarAviso(PedidoEventoKafka e) {
        String texto = switch (e.tipo()) {
            case "PEDIDO_CREADO" -> "Recibimos tu pedido #" + e.pedidoId() + " por $" + e.total();
            case "PEDIDO_ACEPTADO" -> "Tu pedido #" + e.pedidoId() + " fue aceptado";
            case "PEDIDO_EN_PREPARACION" -> "Estamos preparando tu pedido #" + e.pedidoId();
            case "PEDIDO_DESPACHADO" -> "Tu pedido #" + e.pedidoId() + " va en camino";
            case "PEDIDO_ENTREGADO" -> "Tu pedido #" + e.pedidoId() + " fue entregado";
            case "PEDIDO_CANCELADO" -> "Tu pedido #" + e.pedidoId() + " fue cancelado";
            default -> "Actualización del pedido #" + e.pedidoId() + " (" + e.tipo() + ")";
        };
        return "cliente " + e.clienteId() + ": " + texto;
    }
}
