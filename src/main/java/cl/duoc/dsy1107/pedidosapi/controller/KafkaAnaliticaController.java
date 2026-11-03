package cl.duoc.dsy1107.pedidosapi.controller;

import java.util.Map;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.dsy1107.pedidosapi.kafka.AnaliticaKafkaConsumer;
import cl.duoc.dsy1107.pedidosapi.kafka.InventarioKafkaConsumer;

// Muestra lo que los consumidores de Kafka han ido calculando a partir de los
// eventos: es la prueba visible de que el flujo productor -> Kafka -> consumidor
// termina en lógica de negocio. Solo Admin.
@RestController
@RequestMapping("/api/analitica")
@PreAuthorize("hasRole('Admin')")
public class KafkaAnaliticaController {

    private final AnaliticaKafkaConsumer analitica;
    private final InventarioKafkaConsumer inventario;

    public KafkaAnaliticaController(AnaliticaKafkaConsumer analitica, InventarioKafkaConsumer inventario) {
        this.analitica = analitica;
        this.inventario = inventario;
    }

    @GetMapping("/pedidos")
    public Map<String, Object> pedidos() {
        return analitica.resumen();
    }

    @GetMapping("/inventario")
    public Map<String, Object> inventario() {
        return inventario.resumen();
    }
}
