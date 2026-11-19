package cl.duoc.dsy1107.pedidosapi.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declara los tópicos como beans. Al arrancar, Spring Boot (KafkaAdmin) crea
 * los que no existan; si ya existen no los toca. Como en el clúster
 * auto.create.topics.enable=false, esta es la forma "por código" de crearlos.
 *
 * Configuración de cada tópico:
 *  - Particiones: 3 y réplicas: 3.
 *  - retention.ms: 7 días (604800000 ms) por defecto. Se puede cambiar con la
 *    variable/propiedad pedidos360.kafka.retention-ms, o en caliente con
 *    PUT /api/kafka-admin/topics/{nombre}/retention.
 *  - cleanup.policy=delete: los mensajes se borran al cumplirse la retención.
 *    Se usa "delete" y no "compact" porque cada evento es un hecho histórico
 *    que se puede releer; con "compact" solo quedaría el último valor por key.
 *    Se puede cambiar con pedidos360.kafka.cleanup-policy.
 *  - min.insync.replicas=2 junto con acks=all del productor: un mensaje solo se
 *    confirma si al menos 2 de las 3 réplicas lo tienen.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic pedidosEventosTopic(
            @Value("${pedidos360.kafka.retention-ms:604800000}") long retencionMs,
            @Value("${pedidos360.kafka.cleanup-policy:delete}") String cleanupPolicy) {
        return crear(KafkaTopics.PEDIDOS_EVENTOS, retencionMs, cleanupPolicy);
    }

    @Bean
    NewTopic inventarioEventosTopic(
            @Value("${pedidos360.kafka.retention-ms:604800000}") long retencionMs,
            @Value("${pedidos360.kafka.cleanup-policy:delete}") String cleanupPolicy) {
        return crear(KafkaTopics.INVENTARIO_EVENTOS, retencionMs, cleanupPolicy);
    }

    /** Tópico de errores: mismas particiones/réplicas, así el mensaje fallido
     *  conserva el número de partición de origen. */
    @Bean
    NewTopic erroresTopic(
            @Value("${pedidos360.kafka.retention-ms:604800000}") long retencionMs,
            @Value("${pedidos360.kafka.cleanup-policy:delete}") String cleanupPolicy) {
        return crear(KafkaTopics.ERRORES, retencionMs, cleanupPolicy);
    }

    private NewTopic crear(String nombre, long retencionMs, String cleanupPolicy) {
        return TopicBuilder.name(nombre)
            .partitions(KafkaTopics.PARTICIONES)
            .replicas(KafkaTopics.REPLICAS)
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retencionMs))
            .config(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy)
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
            .build();
    }
}
