package cl.duoc.dsy1107.pedidosapi.kafka;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import cl.duoc.dsy1107.pedidosapi.kafka.dto.PedidoEventoKafka;

/**
 * Consumer group "pedidos360-analitica" sobre pedidos.eventos.
 *
 * Lee los MISMOS eventos que el grupo de notificaciones, pero como es otro
 * grupo, Kafka le entrega todos los mensajes a él también (cada grupo lleva
 * su propio offset). Aquí acumula métricas en memoria: eventos por tipo,
 * pedidos aceptados e ingresos.
 *
 * Es idempotente: guarda los últimos eventId vistos y descarta duplicados
 * (Kafka garantiza "al menos una vez", así que un reintento podría repetir
 * un mensaje y no queremos sumar dos veces el mismo ingreso).
 *
 * Las métricas viven en memoria: se reinician al reiniciar el backend. Como
 * el grupo arranca desde el offset guardado en Kafka, solo recibe lo nuevo.
 */
@Component
public class AnaliticaKafkaConsumer {

    private static final int MAX_IDS_RECORDADOS = 1000;

    private final Map<String, Long> eventosPorTipo = new TreeMap<>();
    private final Map<String, Boolean> idsVistos = new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > MAX_IDS_RECORDADOS;
        }
    };

    private long eventosProcesados = 0;
    private long duplicadosIgnorados = 0;
    private long pedidosAceptados = 0;
    private BigDecimal ingresosAceptados = BigDecimal.ZERO;

    @KafkaListener(
        topics = KafkaTopics.PEDIDOS_EVENTOS,
        groupId = KafkaGrupos.ANALITICA,
        concurrency = "1")
    public void consumir(ConsumerRecord<String, PedidoEventoKafka> record) {
        PedidoEventoKafka evento = record.value();
        validar(evento);

        synchronized (this) {
            if (evento.eventId() != null && idsVistos.put(evento.eventId(), Boolean.TRUE) != null) {
                duplicadosIgnorados++;
                System.out.println("[KAFKA][ANALITICA] duplicado ignorado eventId=" + evento.eventId());
                return;
            }

            eventosProcesados++;
            eventosPorTipo.merge(evento.tipo(), 1L, Long::sum);

            if ("PEDIDO_ACEPTADO".equals(evento.tipo())) {
                pedidosAceptados++;
                if (evento.total() != null) {
                    ingresosAceptados = ingresosAceptados.add(evento.total());
                }
            }
        }

        System.out.println("[KAFKA][ANALITICA] particion=" + record.partition()
            + " offset=" + record.offset() + " tipo=" + evento.tipo()
            + " pedidoId=" + evento.pedidoId());
    }

    /** Resumen para GET /api/analitica/pedidos. */
    public synchronized Map<String, Object> resumen() {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("grupo", KafkaGrupos.ANALITICA);
        r.put("eventosProcesados", eventosProcesados);
        r.put("duplicadosIgnorados", duplicadosIgnorados);
        r.put("eventosPorTipo", new TreeMap<>(eventosPorTipo));
        r.put("pedidosAceptados", pedidosAceptados);
        r.put("ingresosAceptados", ingresosAceptados);
        return r;
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
}
