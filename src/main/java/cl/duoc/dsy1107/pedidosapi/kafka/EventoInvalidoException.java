package cl.duoc.dsy1107.pedidosapi.kafka;

/**
 * Un evento llegó bien deserializado pero con datos que no tienen sentido
 * (sin id de pedido, sin tipo, cantidad negativa...). Reintentar no lo
 * arreglaría, así que el manejador de errores lo manda directo al tópico de
 * errores sin pasar por los reintentos (ver KafkaErrorConfig).
 */
public class EventoInvalidoException extends RuntimeException {

    public EventoInvalidoException(String mensaje) {
        super(mensaje);
    }
}
