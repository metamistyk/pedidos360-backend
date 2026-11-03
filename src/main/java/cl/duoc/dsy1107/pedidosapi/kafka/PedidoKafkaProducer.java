package cl.duoc.dsy1107.pedidosapi.kafka;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import cl.duoc.dsy1107.pedidosapi.domain.ItemPedido;
import cl.duoc.dsy1107.pedidosapi.domain.Pedido;
import cl.duoc.dsy1107.pedidosapi.kafka.dto.InventarioEventoKafka;
import cl.duoc.dsy1107.pedidosapi.kafka.dto.ItemEventoKafka;
import cl.duoc.dsy1107.pedidosapi.kafka.dto.PedidoEventoKafka;

/**
 * Productor de Kafka. El negocio (PedidoService) solo llama a estos métodos;
 * aquí se arma el DTO, se elige la KEY y se envía con KafkaTemplate.
 *
 * Es complementario a PedidoEventoProducer (RabbitMQ), que sigue funcionando
 * igual: RabbitMQ reparte tareas con ACK/DLQ; Kafka guarda el flujo de
 * eventos como registro que se puede releer.
 *
 * Si Kafka no está disponible, se registra el error pero NO se interrumpe el
 * pedido: el negocio no depende de que Kafka esté arriba.
 */
@Component
public class PedidoKafkaProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PedidoKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Evento de cambio de estado del pedido. Key = id del pedido. */
    public void publicarPedido(String tipo, Pedido pedido) {
        List<ItemEventoKafka> items = pedido.getItems().stream()
            .map(i -> new ItemEventoKafka(
                i.getProducto().getId(),
                i.getProducto().getNombre(),
                i.getCantidad(),
                i.getPrecioUnitario()))
            .toList();

        BigDecimal total = pedido.getItems().stream()
            .map(i -> i.getPrecioUnitario().multiply(BigDecimal.valueOf(i.getCantidad())))
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        PedidoEventoKafka evento = new PedidoEventoKafka(
            UUID.randomUUID().toString(),
            tipo,
            pedido.getId(),
            pedido.getEstado().name(),
            pedido.getClienteId(),
            total,
            items,
            LocalDateTime.now().toString());

        enviar(KafkaTopics.PEDIDOS_EVENTOS, String.valueOf(pedido.getId()), evento);
    }

    /** Evento de stock descontado de un producto. Key = id del producto. */
    public void publicarStockDescontado(Pedido pedido, ItemPedido item) {
        InventarioEventoKafka evento = new InventarioEventoKafka(
            UUID.randomUUID().toString(),
            "STOCK_DESCONTADO",
            item.getProducto().getId(),
            item.getProducto().getNombre(),
            item.getCantidad(),
            item.getProducto().getStock(),
            pedido.getId(),
            LocalDateTime.now().toString());

        enviar(KafkaTopics.INVENTARIO_EVENTOS, String.valueOf(item.getProducto().getId()), evento);
    }

    private void enviar(String topico, String key, Object evento) {
        try {
            kafkaTemplate.send(topico, key, evento).whenComplete((resultado, error) -> {
                if (error != null) {
                    System.out.println("[KAFKA][PRODUCER][ERROR] topico=" + topico
                        + " key=" + key + " - " + error.getMessage());
                } else {
                    RecordMetadata meta = resultado.getRecordMetadata();
                    System.out.println("[KAFKA][PRODUCER] topico=" + meta.topic()
                        + " key=" + key
                        + " particion=" + meta.partition()
                        + " offset=" + meta.offset());
                }
            });
        } catch (Exception e) {
            // Kafka caído o sin metadatos: se registra y el pedido sigue su curso.
            System.out.println("[KAFKA][PRODUCER][ERROR] no se pudo enviar a " + topico + " - " + e.getMessage());
        }
    }
}
