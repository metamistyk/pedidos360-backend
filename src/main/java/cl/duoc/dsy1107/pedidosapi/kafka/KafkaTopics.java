package cl.duoc.dsy1107.pedidosapi.kafka;

/**
 * Nombres de los tópicos de Kafka, centralizados en un solo lugar
 * (igual que RabbitMQConfig lo hace con las colas y exchanges).
 *
 * Kafka complementa a RabbitMQ: RabbitMQ reparte tareas (notificar, cocina,
 * despacho) con ACK/DLQ; Kafka guarda el flujo de eventos de dominio como un
 * registro que se puede releer (retención, offsets, consumer groups).
 */
public final class KafkaTopics {

    private KafkaTopics() {
    }

    /** Cada cambio de estado de un pedido. Key = id del pedido. */
    public static final String PEDIDOS_EVENTOS = "pedidos.eventos";

    /** Movimientos de stock por producto. Key = id del producto. */
    public static final String INVENTARIO_EVENTOS = "inventario.eventos";

    /**
     * Tópico auxiliar de errores (equivalente a la DLQ de RabbitMQ): recibe el
     * mensaje que agotó sus reintentos en un consumidor, con el motivo del
     * fallo. No es un tópico de negocio.
     */
    public static final String ERRORES = "pedidos360.errores";

    /** Particiones y réplicas exigidas por la pauta: 3 y 3. */
    public static final int PARTICIONES = 3;
    public static final int REPLICAS = 3;
}
