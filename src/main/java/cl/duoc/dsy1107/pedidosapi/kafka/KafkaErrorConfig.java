package cl.duoc.dsy1107.pedidosapi.kafka;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import cl.duoc.dsy1107.pedidosapi.kafka.dto.ErrorEventoKafka;

/**
 * Manejo de errores de TODOS los @KafkaListener.
 *
 * Spring Boot toma este bean (CommonErrorHandler) y lo aplica solo a los
 * consumidores. Qué pasa cuando un listener lanza una excepción:
 *
 *   1. Se reintenta el mismo mensaje con espera creciente (1 s, 2 s, 4 s...).
 *      Reintentos: pedidos360.kafka.reintentos (3). Espera inicial:
 *      pedidos360.kafka.backoff-ms (1000).
 *   2. Si se agotan, el mensaje se publica en "pedidos360.errores" con el
 *      motivo, y el consumidor sigue con el siguiente (no se queda trabado).
 *   3. EventoInvalidoException y los mensajes que no se pueden deserializar
 *      NO se reintentan: van directo al tópico de errores.
 *
 * Es el equivalente a retry + DLQ de RabbitMQ, pero en Kafka.
 */
@Configuration
public class KafkaErrorConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${pedidos360.kafka.reintentos:3}") int reintentos,
            @Value("${pedidos360.kafka.backoff-ms:1000}") long backoffMs) {

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(reintentos);
        backOff.setInitialInterval(backoffMs);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(publicarEnErrores(kafkaTemplate), backOff);
        handler.addNotRetryableExceptions(EventoInvalidoException.class);
        // Al agotar reintentos se confirma el offset del mensaje descartado:
        // si el backend se reinicia, no vuelve a procesarlo.
        handler.setCommitRecovered(true);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                System.out.println("[KAFKA][CONSUMER][REINTENTO] intento " + deliveryAttempt
                    + " fallido topico=" + record.topic() + " particion=" + record.partition()
                    + " offset=" + record.offset() + " - " + causa(ex));
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                System.out.println("[KAFKA][CONSUMER][ERRORES] mensaje descartado topico=" + record.topic()
                    + " particion=" + record.partition() + " offset=" + record.offset()
                    + " -> " + KafkaTopics.ERRORES);
            }
        });
        return handler;
    }

    /** Publica el mensaje fallido, con su contexto, en el tópico de errores. */
    private ConsumerRecordRecoverer publicarEnErrores(KafkaTemplate<String, Object> kafkaTemplate) {
        return (record, ex) -> {
            String key = record.key() == null ? null : String.valueOf(record.key());
            String valor = record.value() == null ? "<no se pudo deserializar>" : String.valueOf(record.value());

            ErrorEventoKafka error = new ErrorEventoKafka(
                record.topic(),
                record.partition(),
                record.offset(),
                key,
                valor,
                causa(ex),
                LocalDateTime.now().toString());

            try {
                // Envío sincrónico: si falla, la excepción hace que el mensaje se reintente.
                kafkaTemplate.send(KafkaTopics.ERRORES, key, error).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("No se pudo publicar en " + KafkaTopics.ERRORES, e);
            }
        };
    }

    /** Texto corto con la causa real: se recorre la cadena de excepciones hasta
     *  la raíz (los listeners y el deserializador envuelven la original). */
    private static String causa(Exception ex) {
        Throwable t = ex;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        String mensaje = String.valueOf(t.getMessage());
        if (mensaje.length() > 300) {
            mensaje = mensaje.substring(0, 300) + "...";
        }
        return t.getClass().getSimpleName() + ": " + mensaje;
    }
}
