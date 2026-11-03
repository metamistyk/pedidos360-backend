package cl.duoc.dsy1107.pedidosapi.kafka;

/**
 * Nombres de los consumer groups. Cada grupo recibe TODOS los mensajes del
 * tópico que escucha; dentro de un mismo grupo, las particiones se reparten
 * entre sus consumidores (cada mensaje lo procesa uno solo).
 */
public final class KafkaGrupos {

    private KafkaGrupos() {
    }

    /** Escucha pedidos.eventos: arma y "envía" avisos al cliente. */
    public static final String NOTIFICACIONES = "pedidos360-notificaciones";

    /** Escucha pedidos.eventos: acumula métricas de pedidos e ingresos. */
    public static final String ANALITICA = "pedidos360-analitica";

    /** Escucha inventario.eventos: sigue el stock y alerta stock bajo. */
    public static final String INVENTARIO = "pedidos360-inventario";
}
