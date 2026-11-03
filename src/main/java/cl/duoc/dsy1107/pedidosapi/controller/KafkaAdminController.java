package cl.duoc.dsy1107.pedidosapi.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.dsy1107.pedidosapi.dto.KafkaRetentionRequest;
import cl.duoc.dsy1107.pedidosapi.dto.KafkaTopicRequest;
import cl.duoc.dsy1107.pedidosapi.kafka.KafkaAdminService;
import cl.duoc.dsy1107.pedidosapi.kafka.KafkaAdminService.GrupoInfo;
import cl.duoc.dsy1107.pedidosapi.kafka.KafkaAdminService.LagGrupo;
import cl.duoc.dsy1107.pedidosapi.kafka.KafkaAdminService.TopicInfo;
import jakarta.validation.Valid;

// Administración de Kafka por API REST: tópicos, retención, consumer groups y
// lag, sin tener que entrar a Kafka UI ni a la consola. Solo Admin puede
// usarlo (mismo criterio que RabbitAdminController).
@RestController
@RequestMapping("/api/kafka-admin")
@PreAuthorize("hasRole('Admin')")
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @GetMapping("/topics")
    public List<String> listarTopics() {
        return kafkaAdminService.listarTopics();
    }

    @GetMapping("/topics/{nombre}")
    public TopicInfo describirTopic(@PathVariable String nombre) {
        return kafkaAdminService.describirTopic(nombre);
    }

    @PostMapping("/topics")
    public ResponseEntity<Map<String, String>> crearTopic(@Valid @RequestBody KafkaTopicRequest request) {
        String nombre = kafkaAdminService.crearTopic(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("topic", nombre));
    }

    @PutMapping("/topics/{nombre}/retention")
    public Map<String, Object> cambiarRetencion(@PathVariable String nombre,
                                                @Valid @RequestBody KafkaRetentionRequest request) {
        kafkaAdminService.cambiarRetencion(nombre, request.getRetencionMs());
        return Map.of("topic", nombre, "retencionMs", request.getRetencionMs());
    }

    @DeleteMapping("/topics/{nombre}")
    public ResponseEntity<Void> eliminarTopic(@PathVariable String nombre) {
        kafkaAdminService.eliminarTopic(nombre);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/groups")
    public List<GrupoInfo> listarGrupos() {
        return kafkaAdminService.listarGrupos();
    }

    @GetMapping("/groups/{grupo}/lag")
    public LagGrupo lagDeGrupo(@PathVariable String grupo) {
        return kafkaAdminService.lagDeGrupo(grupo);
    }

    // Errores de entrada (topico inexistente, ya existe, parametros invalidos) -> 400.
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> manejarValidacion(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }

    // Kafka caido o sin respuesta -> 503 (no es culpa de quien llama a la API).
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> manejarKafkaNoDisponible(IllegalStateException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", ex.getMessage()));
    }
}
