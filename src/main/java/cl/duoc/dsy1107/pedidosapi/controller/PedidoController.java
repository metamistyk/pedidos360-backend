package cl.duoc.dsy1107.pedidosapi.controller;

import cl.duoc.dsy1107.pedidosapi.domain.Pedido;
import cl.duoc.dsy1107.pedidosapi.dto.CambiarEstadoRequest;
import cl.duoc.dsy1107.pedidosapi.dto.CrearPedidoRequest;
import cl.duoc.dsy1107.pedidosapi.service.PedidoService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
public class PedidoController {

    private final PedidoService pedidoService;

    public PedidoController(PedidoService pedidoService) {
        this.pedidoService = pedidoService;
    }

    @GetMapping("/publico")
    public String publico() {
        return "Este endpoint es público, no requiere token.";
    }

    @PostMapping("/orders")
    @PreAuthorize("hasAnyRole('Cliente', 'Operador', 'Admin')")
    public Pedido crear(@RequestBody CrearPedidoRequest request, Authentication auth) {
        String clienteId = obtenerOid(auth);
        return pedidoService.crear(clienteId, request);
    }

    @GetMapping("/orders")
    public List<Pedido> listar(Authentication auth) {
        String clienteId = obtenerOid(auth);
        boolean verTodos = tieneRol(auth, "Operador") || tieneRol(auth, "Admin");
        return pedidoService.listar(clienteId, verTodos);
    }

    @GetMapping("/orders/{id}")
    public Pedido obtener(@PathVariable Long id, Authentication auth) {
        String clienteId = obtenerOid(auth);
        boolean verTodos = tieneRol(auth, "Operador") || tieneRol(auth, "Admin");
        return pedidoService.obtener(id, clienteId, verTodos);
    }

    @PutMapping("/orders/{id}/status")
    @PreAuthorize("hasAnyRole('Operador', 'Admin')")
    public Pedido cambiarEstado(@PathVariable Long id, @RequestBody CambiarEstadoRequest request) {
        return pedidoService.cambiarEstado(id, request.getNuevoEstado());
    }

    private String obtenerOid(Authentication auth) {
        Jwt jwt = ((JwtAuthenticationToken) auth).getToken();
        return jwt.getClaimAsString("oid");
    }

    private boolean tieneRol(Authentication auth, String rol) {
        return auth.getAuthorities().stream()
            .map(GrantedAuthority::getAuthority)
            .anyMatch(a -> a.equals("ROLE_" + rol));
    }
}