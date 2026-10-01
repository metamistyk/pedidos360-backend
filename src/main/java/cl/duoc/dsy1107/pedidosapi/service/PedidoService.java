package cl.duoc.dsy1107.pedidosapi.service;

import cl.duoc.dsy1107.pedidosapi.domain.*;
import cl.duoc.dsy1107.pedidosapi.dto.CrearPedidoRequest;
import cl.duoc.dsy1107.pedidosapi.dto.ItemRequest;
import cl.duoc.dsy1107.pedidosapi.repository.PedidoRepository;
import cl.duoc.dsy1107.pedidosapi.repository.ProductoRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class PedidoService {

    private final PedidoRepository pedidoRepository;
    private final ProductoRepository productoRepository;

    // Mapa de transiciones válidas: desde cada estado, a qué estados se puede avanzar.
    private static final Map<EstadoPedido, Set<EstadoPedido>> TRANSICIONES = Map.of(
        EstadoPedido.CREADO,         Set.of(EstadoPedido.ACEPTADO, EstadoPedido.CANCELADO),
        EstadoPedido.ACEPTADO,       Set.of(EstadoPedido.EN_PREPARACION, EstadoPedido.CANCELADO),
        EstadoPedido.EN_PREPARACION, Set.of(EstadoPedido.DESPACHADO, EstadoPedido.CANCELADO),
        EstadoPedido.DESPACHADO,     Set.of(EstadoPedido.ENTREGADO),
        EstadoPedido.ENTREGADO,      Set.of(),
        EstadoPedido.CANCELADO,      Set.of()
    );

    public PedidoService(PedidoRepository pedidoRepository, ProductoRepository productoRepository) {
        this.pedidoRepository = pedidoRepository;
        this.productoRepository = productoRepository;
    }

    public Pedido crear(String clienteId, CrearPedidoRequest request) {
        Pedido pedido = new Pedido(clienteId);

        for (ItemRequest itemReq : request.getItems()) {
            Producto producto = productoRepository.findById(itemReq.getProductoId())
                .orElseThrow(() -> new RuntimeException("Producto no encontrado: " + itemReq.getProductoId()));

            ItemPedido item = new ItemPedido(producto, itemReq.getCantidad(), producto.getPrecio());
            pedido.agregarItem(item);
        }

        return pedidoRepository.save(pedido);
    }

    // Cliente ve solo lo suyo; Operador/Admin ven todo.
    public List<Pedido> listar(String clienteId, boolean verTodos) {
        if (verTodos) {
            return pedidoRepository.findAll();
        }
        return pedidoRepository.findByClienteId(clienteId);
    }

    public Pedido obtener(Long id, String clienteId, boolean verTodos) {
        Pedido pedido = pedidoRepository.findById(id)
            .orElseThrow(() -> new RuntimeException("Pedido no encontrado: " + id));

        if (!verTodos && !pedido.getClienteId().equals(clienteId)) {
            throw new SecurityException("No tienes acceso a este pedido");
        }

        return pedido;
    }

    public Pedido cambiarEstado(Long id, EstadoPedido nuevoEstado) {
        Pedido pedido = pedidoRepository.findById(id)
            .orElseThrow(() -> new RuntimeException("Pedido no encontrado: " + id));

        EstadoPedido estadoActual = pedido.getEstado();

        // Regla de negocio: solo se permiten las transiciones definidas en el mapa de estados.
        if (!TRANSICIONES.get(estadoActual).contains(nuevoEstado)) {
            throw new IllegalStateException(
                "Transición inválida: " + estadoActual + " → " + nuevoEstado);
        }

        // Regla de negocio: al aceptar, se descuenta el stock.
        if (nuevoEstado == EstadoPedido.ACEPTADO) {
            for (ItemPedido item : pedido.getItems()) {
                Producto producto = item.getProducto();
                int stockRestante = producto.getStock() - item.getCantidad();

                if (stockRestante < 0) {
                    throw new IllegalStateException("Stock insuficiente para: " + producto.getNombre());
                }

                producto.setStock(stockRestante);
                productoRepository.save(producto);
            }
        }

        pedido.setEstado(nuevoEstado);
        return pedidoRepository.save(pedido);
    }
}