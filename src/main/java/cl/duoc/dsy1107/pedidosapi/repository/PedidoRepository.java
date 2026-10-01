package cl.duoc.dsy1107.pedidosapi.repository;

import cl.duoc.dsy1107.pedidosapi.domain.Pedido;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface PedidoRepository extends JpaRepository<Pedido, Long> {

    // Para que un Cliente solo vea sus propios pedidos.
    List<Pedido> findByClienteId(String clienteId);
}