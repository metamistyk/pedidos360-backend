package cl.duoc.dsy1107.pedidosapi.kafka;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import cl.duoc.dsy1107.pedidosapi.kafka.dto.InventarioEventoKafka;

/**
 * Consumer group "pedidos360-inventario" sobre inventario.eventos.
 *
 * Sigue el último stock conocido de cada producto y emite una alerta cuando
 * baja del mínimo (pedidos360.kafka.stock-minimo, por defecto 10).
 *
 * La key del mensaje es el id del producto, así que todos los movimientos de
 * un producto llegan en orden y el "último stock" siempre es el correcto.
 */
@Component
public class InventarioKafkaConsumer {

    private final int stockMinimo;
    private final Map<Long, Integer> stockPorProducto = new TreeMap<>();
    private long movimientosProcesados = 0;
    private long alertasStockBajo = 0;

    public InventarioKafkaConsumer(@Value("${pedidos360.kafka.stock-minimo:10}") int stockMinimo) {
        this.stockMinimo = stockMinimo;
    }

    @KafkaListener(
        topics = KafkaTopics.INVENTARIO_EVENTOS,
        groupId = KafkaGrupos.INVENTARIO,
        concurrency = "1")
    public void consumir(ConsumerRecord<String, InventarioEventoKafka> record) {
        InventarioEventoKafka evento = record.value();
        validar(evento);

        boolean stockBajo = evento.stockRestante() < stockMinimo;
        synchronized (this) {
            movimientosProcesados++;
            stockPorProducto.put(evento.productoId(), evento.stockRestante());
            if (stockBajo) {
                alertasStockBajo++;
            }
        }

        System.out.println("[KAFKA][INVENTARIO] particion=" + record.partition()
            + " offset=" + record.offset() + " producto=" + evento.productoId()
            + " (" + evento.nombre() + ") descontado=" + evento.cantidad()
            + " stock=" + evento.stockRestante());

        if (stockBajo) {
            System.out.println("[KAFKA][INVENTARIO][ALERTA] stock bajo de " + evento.nombre()
                + ": quedan " + evento.stockRestante() + " (mínimo " + stockMinimo + ")");
        }
    }

    /** Resumen para GET /api/analitica/inventario. */
    public synchronized Map<String, Object> resumen() {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("grupo", KafkaGrupos.INVENTARIO);
        r.put("movimientosProcesados", movimientosProcesados);
        r.put("alertasStockBajo", alertasStockBajo);
        r.put("stockMinimo", stockMinimo);
        r.put("stockPorProducto", new TreeMap<>(stockPorProducto));
        return r;
    }

    private void validar(InventarioEventoKafka e) {
        if (e == null) {
            throw new EventoInvalidoException("evento vacío");
        }
        if (e.productoId() == null) {
            throw new EventoInvalidoException("evento sin productoId (eventId=" + e.eventId() + ")");
        }
        if (e.cantidad() <= 0) {
            throw new EventoInvalidoException("cantidad inválida (" + e.cantidad() + ") en producto " + e.productoId());
        }
    }
}
