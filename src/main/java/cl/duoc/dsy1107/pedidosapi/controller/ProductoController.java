package cl.duoc.dsy1107.pedidosapi.controller;

import cl.duoc.dsy1107.pedidosapi.domain.Producto;
import cl.duoc.dsy1107.pedidosapi.dto.ProductoRequest;
import cl.duoc.dsy1107.pedidosapi.service.ProductoService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/catalog/products")
public class ProductoController {

    private final ProductoService productoService;

    public ProductoController(ProductoService productoService) {
        this.productoService = productoService;
    }

    @GetMapping
    public List<Producto> listar() {
        return productoService.listar();
    }

    @PostMapping
    @PreAuthorize("hasRole('Admin')")
    public Producto crear(@RequestBody ProductoRequest request) {
        return productoService.crear(request);
    }

    @PutMapping("/{id}")
    @PreAuthorize("hasRole('Admin')")
    public Producto actualizar(@PathVariable Long id, @RequestBody ProductoRequest request) {
        return productoService.actualizar(id, request);
    }
}