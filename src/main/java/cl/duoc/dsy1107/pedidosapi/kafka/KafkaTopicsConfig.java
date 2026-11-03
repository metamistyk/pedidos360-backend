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
 * Retención por defecto: 7 días (604800000 ms). Se puede cambiar con la
 * variable/propiedad pedidos360.kafka.retention-ms, o en caliente con
 * PUT /api/kafka-admin/topics/{nombre}/retention.
 *
 * min.insync.replicas=2 junto con acks=all del productor: un mensaje solo se
 * confirma si al menos 2 de las 3 réplicas lo tienen.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic pedidosEventosTopic(@Value("${pedidos360.kafka.retention-ms:604800000}") long retencionMs) {
        return crear(KafkaTopics.PEDIDOS_EVENTOS, retencionMs);
    }

    @Bean
    NewTopic inventarioEventosTopic(@Value("${pedidos360.kafka.retention-ms:604800000}") long retencionMs) {
        return crear(KafkaTopics.INVENTARIO_EVENTOS, retencionMs);
    }

    /** Tópico de errores: mismas particiones/réplicas, así el mensaje fallido
     *  conserva el número de partición de origen. */
    @Bean
    NewTopic erroresTopic(@Value("${pedidos360.kafka.retention-ms:604800000}") long retencionMs) {
        return crear(KafkaTopics.ERRORES, retencionMs);
    }

    private NewTopic crear(String nombre, long retencionMs) {
        return TopicBuilder.name(nombre)
            .partitions(KafkaTopics.PARTICIONES)
            .replicas(KafkaTopics.REPLICAS)
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retencionMs))
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
            .build();
    }
}
