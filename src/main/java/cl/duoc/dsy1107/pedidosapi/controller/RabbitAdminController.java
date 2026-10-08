package cl.duoc.dsy1107.pedidosapi.controller;

import cl.duoc.dsy1107.pedidosapi.admin.RabbitAdminService;
import cl.duoc.dsy1107.pedidosapi.dto.AdminBindingRequest;
import cl.duoc.dsy1107.pedidosapi.dto.AdminExchangeRequest;
import cl.duoc.dsy1107.pedidosapi.dto.AdminQueueRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

// Microservicio administrador de RabbitMQ: crear/eliminar queues y exchanges,
// y declarar bindings, sin tener que entrar a la Management UI. Solo Admin
// puede usarlo (mismo criterio que ProductoController).
@RestController
@RequestMapping("/api/admin")
@PreAuthorize("hasRole('Admin')")
public class RabbitAdminController {

    private final RabbitAdminService rabbitAdminService;

    public RabbitAdminController(RabbitAdminService rabbitAdminService) {
        this.rabbitAdminService = rabbitAdminService;
    }

    @PostMapping("/queues")
    public ResponseEntity<Map<String, String>> crearQueue(@Valid @RequestBody AdminQueueRequest request) {
        String nombre = rabbitAdminService.crearQueue(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("queue", nombre));
    }

    @DeleteMapping("/queues/{nombre}")
    public ResponseEntity<Void> eliminarQueue(@PathVariable String nombre) {
        rabbitAdminService.eliminarQueue(nombre);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/exchanges")
    public ResponseEntity<Map<String, String>> crearExchange(@Valid @RequestBody AdminExchangeRequest request) {
        String nombre = rabbitAdminService.crearExchange(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("exchange", nombre));
    }

    @DeleteMapping("/exchanges/{nombre}")
    public ResponseEntity<Void> eliminarExchange(@PathVariable String nombre) {
        rabbitAdminService.eliminarExchange(nombre);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/bindings")
    public ResponseEntity<Void> crearBinding(@Valid @RequestBody AdminBindingRequest request) {
        rabbitAdminService.crearBinding(request);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    // Convierte los IllegalArgumentException del service (nombres vacios,
    // tipo de exchange invalido, cola/exchange inexistente al eliminar) en
    // un 400 limpio en vez del 500 generico por defecto.
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> manejarValidacion(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }
}