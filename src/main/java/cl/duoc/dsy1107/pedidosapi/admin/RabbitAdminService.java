package cl.duoc.dsy1107.pedidosapi.admin;

import cl.duoc.dsy1107.pedidosapi.dto.AdminBindingRequest;
import cl.duoc.dsy1107.pedidosapi.dto.AdminExchangeRequest;
import cl.duoc.dsy1107.pedidosapi.dto.AdminQueueRequest;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.stereotype.Service;

// Encapsula TODA la interacción administrativa con RabbitMQ (crear/eliminar
// queues y exchanges, gestionar bindings). El controller solo valida el DTO
// de entrada (@Valid) y delega aquí — así la lógica de mensajería administrativa
// queda separada tanto del negocio (PedidoService) como de la capa HTTP.
@Service
public class RabbitAdminService {

    private final AmqpAdmin amqpAdmin;

    public RabbitAdminService(AmqpAdmin amqpAdmin) {
        this.amqpAdmin = amqpAdmin;
    }

    public String crearQueue(AdminQueueRequest request) {
        validarNoVacio(request.getNombre(), "nombre de la cola");

        Queue queue = new Queue(request.getNombre(), request.isDurable());
        return amqpAdmin.declareQueue(queue);
    }

    public void eliminarQueue(String nombre) {
        validarNoVacio(nombre, "nombre de la cola");

        boolean eliminada = amqpAdmin.deleteQueue(nombre);
        if (!eliminada) {
            throw new IllegalArgumentException("No existe la cola: " + nombre);
        }
    }

    public String crearExchange(AdminExchangeRequest request) {
        validarNoVacio(request.getNombre(), "nombre del exchange");
        validarNoVacio(request.getTipo(), "tipo de exchange");

        Exchange exchange = switch (request.getTipo().toLowerCase()) {
            case "direct" -> new DirectExchange(request.getNombre(), request.isDurable(), false);
            case "topic" -> new TopicExchange(request.getNombre(), request.isDurable(), false);
            case "fanout" -> new FanoutExchange(request.getNombre(), request.isDurable(), false);
            default -> throw new IllegalArgumentException(
                "Tipo de exchange invalido: " + request.getTipo() + " (use direct, topic o fanout)");
        };

        amqpAdmin.declareExchange(exchange);
        return request.getNombre();
    }

    public void eliminarExchange(String nombre) {
        validarNoVacio(nombre, "nombre del exchange");

        boolean eliminado = amqpAdmin.deleteExchange(nombre);
        if (!eliminado) {
            throw new IllegalArgumentException("No existe el exchange: " + nombre);
        }
    }

    public void crearBinding(AdminBindingRequest request) {
        validarNoVacio(request.getExchange(), "nombre del exchange");
        validarNoVacio(request.getQueue(), "nombre de la cola");
        validarNoVacio(request.getRoutingKey(), "routing key");

        // Constructor genérico de Binding: no necesita saber el tipo real del
        // exchange (direct/topic/fanout) en tiempo de compilación, a diferencia
        // de BindingBuilder.bind(...).to(DirectExchange) o .to(TopicExchange).
        Binding binding = new Binding(
            request.getQueue(),
            Binding.DestinationType.QUEUE,
            request.getExchange(),
            request.getRoutingKey(),
            null
        );

        amqpAdmin.declareBinding(binding);
    }

    private void validarNoVacio(String valor, String campo) {
        if (valor == null || valor.isBlank()) {
            throw new IllegalArgumentException("El " + campo + " no puede estar vacio");
        }
    }
}