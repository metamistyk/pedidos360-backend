package cl.duoc.dsy1107.pedidosapi.repository;

import cl.duoc.dsy1107.pedidosapi.domain.Producto;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductoRepository extends JpaRepository<Producto, Long> {
}