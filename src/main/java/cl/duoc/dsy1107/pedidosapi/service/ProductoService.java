package cl.duoc.dsy1107.pedidosapi.service;

import cl.duoc.dsy1107.pedidosapi.domain.Producto;
import cl.duoc.dsy1107.pedidosapi.dto.ProductoRequest;
import cl.duoc.dsy1107.pedidosapi.repository.ProductoRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProductoService {

    private final ProductoRepository productoRepository;

    public ProductoService(ProductoRepository productoRepository) {
        this.productoRepository = productoRepository;
    }

    public List<Producto> listar() {
        return productoRepository.findAll();
    }

    public Producto crear(ProductoRequest request) {
        Producto producto = new Producto(request.getNombre(), request.getPrecio(), request.getStock());
        return productoRepository.save(producto);
    }

    public Producto actualizar(Long id, ProductoRequest request) {
        Producto producto = productoRepository.findById(id)
            .orElseThrow(() -> new RuntimeException("Producto no encontrado: " + id));

        producto.setNombre(request.getNombre());
        producto.setPrecio(request.getPrecio());
        producto.setStock(request.getStock());

        return productoRepository.save(producto);
    }
}