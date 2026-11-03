package cl.duoc.dsy1107.pedidosapi.kafka;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import cl.duoc.dsy1107.pedidosapi.dto.KafkaTopicRequest;

/**
 * Administración de Kafka por código usando el cliente Admin (AdminClient):
 * listar/describir/crear/eliminar tópicos, cambiar la retención en caliente y
 * observar consumer groups y su lag.
 *
 * El controller solo valida la entrada y delega aquí; esta clase concentra
 * toda la interacción administrativa con el clúster (misma idea que
 * RabbitAdminService).
 */
@Service
public class KafkaAdminService {

    private static final long TIMEOUT_SEG = 10;

    private final KafkaAdmin kafkaAdmin;

    public KafkaAdminService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    // ---------------------------------------------------------------- modelos

    public record PartitionDetalle(int particion, int lider, List<Integer> replicas, List<Integer> isr) {
    }

    public record TopicInfo(String nombre, int particiones, int factorReplicacion,
                            String retencionMs, String minInsyncReplicas,
                            List<PartitionDetalle> detalle) {
    }

    public record GrupoInfo(String grupo, String estado, int miembros) {
    }

    public record LagDetalle(String topico, int particion, long offsetConfirmado,
                             long offsetFinal, long lag) {
    }

    public record LagGrupo(String grupo, long lagTotal, List<LagDetalle> particiones) {
    }

    // ----------------------------------------------------------------- topics

    public List<String> listarTopics() {
        try (Admin admin = admin()) {
            return admin.listTopics().names().get(TIMEOUT_SEG, TimeUnit.SECONDS)
                .stream().sorted().toList();
        } catch (Exception e) {
            throw traducir(e);
        }
    }

    public TopicInfo describirTopic(String nombre) {
        validarNoVacio(nombre, "nombre del topico");

        try (Admin admin = admin()) {
            TopicDescription descripcion = admin.describeTopics(List.of(nombre))
                .allTopicNames().get(TIMEOUT_SEG, TimeUnit.SECONDS).get(nombre);

            ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
            Config config = admin.describeConfigs(List.of(recurso))
                .all().get(TIMEOUT_SEG, TimeUnit.SECONDS).get(recurso);

            List<PartitionDetalle> detalle = descripcion.partitions().stream()
                .map(p -> new PartitionDetalle(
                    p.partition(),
                    p.leader() == null ? -1 : p.leader().id(),
                    p.replicas().stream().map(Node::id).toList(),
                    p.isr().stream().map(Node::id).toList()))
                .toList();

            int factor = descripcion.partitions().isEmpty()
                ? 0
                : descripcion.partitions().get(0).replicas().size();

            return new TopicInfo(
                nombre,
                descripcion.partitions().size(),
                factor,
                valor(config, "retention.ms"),
                valor(config, "min.insync.replicas"),
                detalle);
        } catch (Exception e) {
            throw traducir(e);
        }
    }

    public String crearTopic(KafkaTopicRequest request) {
        validarNoVacio(request.getNombre(), "nombre del topico");

        Map<String, String> configuracion = new HashMap<>();
        configuracion.put("retention.ms", String.valueOf(request.getRetencionMs()));
        // Con 3 réplicas se exigen 2 en sincronía; con menos, el máximo posible.
        configuracion.put("min.insync.replicas",
            String.valueOf(Math.min(2, request.getFactorReplicacion())));

        NewTopic topic = new NewTopic(
            request.getNombre(),
            request.getParticiones(),
            (short) request.getFactorReplicacion()).configs(configuracion);

        try (Admin admin = admin()) {
            admin.createTopics(List.of(topic)).all().get(TIMEOUT_SEG, TimeUnit.SECONDS);
            return request.getNombre();
        } catch (Exception e) {
            throw traducir(e);
        }
    }

    public void cambiarRetencion(String nombre, long retencionMs) {
        validarNoVacio(nombre, "nombre del topico");
        if (retencionMs < 1) {
            throw new IllegalArgumentException("La retencion (ms) debe ser mayor a 0");
        }

        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Collection<AlterConfigOp> operaciones = List.of(new AlterConfigOp(
            new ConfigEntry("retention.ms", String.valueOf(retencionMs)),
            AlterConfigOp.OpType.SET));

        try (Admin admin = admin()) {
            admin.incrementalAlterConfigs(Map.of(recurso, operaciones))
                .all().get(TIMEOUT_SEG, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw traducir(e);
        }
    }

    public void eliminarTopic(String nombre) {
        validarNoVacio(nombre, "nombre del topico");

        // Los tópicos del negocio y el de errores no se pueden borrar desde la API.
        if (KafkaTopics.PEDIDOS_EVENTOS.equals(nombre)
                || KafkaTopics.INVENTARIO_EVENTOS.equals(nombre)
                || KafkaTopics.ERRORES.equals(nombre)) {
            throw new IllegalArgumentException("El topico " + nombre + " esta protegido y no se puede eliminar");
        }

        try (Admin admin = admin()) {
            admin.deleteTopics(List.of(nombre)).all().get(TIMEOUT_SEG, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw traducir(e);
        }
    }

    // ---------------------------------------------------- consumer groups, lag

    public List<GrupoInfo> listarGrupos() {
        try (Admin admin = admin()) {
            List<String> ids = admin.listConsumerGroups().all().get(TIMEOUT_SEG, TimeUnit.SECONDS)
                .stream().map(ConsumerGroupListing::groupId).sorted().toList();

            if (ids.isEmpty()) {
                return List.of();
            }

            Map<String, ConsumerGroupDescription> descripciones = admin.describeConsumerGroups(ids)
                .all().get(TIMEOUT_SEG, TimeUnit.SECONDS);

            return ids.stream()
                .map(id -> new GrupoInfo(
                    id,
                    String.valueOf(descripciones.get(id).state()),
                    descripciones.get(id).members().size()))
                .toList();
        } catch (Exception e) {
            throw traducir(e);
        }
    }

    /**
     * Lag = offset final del log (último mensaje escrito) menos el offset que
     * el grupo ya confirmó. Si es 0, el grupo va al día.
     */
    public LagGrupo lagDeGrupo(String grupo) {
        validarNoVacio(grupo, "id del grupo");

        try (Admin admin = admin()) {
            Map<TopicPartition, OffsetAndMetadata> confirmados = admin.listConsumerGroupOffsets(grupo)
                .partitionsToOffsetAndMetadata().get(TIMEOUT_SEG, TimeUnit.SECONDS);

            // Offset confirmado por partición. Una partición a la que el grupo
            // aún no ha confirmado nada no aparece en listConsumerGroupOffsets,
            // así que también se agregan las particiones que tiene ASIGNADAS
            // (con confirmado 0) para ver las 3 particiones del tópico.
            Map<TopicPartition, Long> confirmadoPorParticion = new HashMap<>();
            confirmados.forEach((tp, offset) -> {
                if (offset != null) {
                    confirmadoPorParticion.put(tp, offset.offset());
                }
            });

            ConsumerGroupDescription descripcion = admin.describeConsumerGroups(List.of(grupo))
                .describedGroups().get(grupo).get(TIMEOUT_SEG, TimeUnit.SECONDS);
            descripcion.members().forEach(miembro ->
                miembro.assignment().topicPartitions().forEach(tp -> confirmadoPorParticion.putIfAbsent(tp, 0L)));

            if (confirmadoPorParticion.isEmpty()) {
                return new LagGrupo(grupo, 0, List.of());
            }

            Map<TopicPartition, OffsetSpec> pedido = new HashMap<>();
            confirmadoPorParticion.keySet().forEach(tp -> pedido.put(tp, OffsetSpec.latest()));

            Map<TopicPartition, ListOffsetsResultInfo> finales = admin.listOffsets(pedido)
                .all().get(TIMEOUT_SEG, TimeUnit.SECONDS);

            List<LagDetalle> detalle = confirmadoPorParticion.keySet().stream()
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                .map(tp -> {
                    long confirmado = confirmadoPorParticion.get(tp);
                    long fin = finales.get(tp).offset();
                    return new LagDetalle(tp.topic(), tp.partition(), confirmado, fin, Math.max(0, fin - confirmado));
                })
                .toList();

            long total = detalle.stream().mapToLong(LagDetalle::lag).sum();
            return new LagGrupo(grupo, total, detalle);
        } catch (Exception e) {
            throw traducir(e);
        }
    }

    // ---------------------------------------------------------------- helpers

    /** Cliente Admin con la misma configuración (brokers) que usa Spring Boot. */
    private Admin admin() {
        return Admin.create(kafkaAdmin.getConfigurationProperties());
    }

    private String valor(Config config, String clave) {
        ConfigEntry entrada = config.get(clave);
        return entrada == null ? null : entrada.value();
    }

    private void validarNoVacio(String valor, String campo) {
        if (valor == null || valor.isBlank()) {
            throw new IllegalArgumentException("El " + campo + " no puede estar vacio");
        }
    }

    /**
     * Errores de entrada del usuario -> IllegalArgumentException (400).
     * Kafka caído o sin respuesta -> IllegalStateException (503).
     */
    private RuntimeException traducir(Exception e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }

        Throwable causa = (e instanceof ExecutionException && e.getCause() != null) ? e.getCause() : e;

        if (causa instanceof UnknownTopicOrPartitionException) {
            return new IllegalArgumentException("No existe el topico");
        }
        if (causa instanceof TopicExistsException) {
            return new IllegalArgumentException("El topico ya existe");
        }
        if (causa instanceof GroupIdNotFoundException) {
            return new IllegalArgumentException("No existe el grupo de consumidores");
        }
        if (causa instanceof InvalidReplicationFactorException
            || causa instanceof InvalidPartitionsException
            || causa instanceof InvalidConfigurationException
            || causa instanceof InvalidTopicException) {
            return new IllegalArgumentException(causa.getMessage());
        }
        if (causa instanceof IllegalArgumentException iae) {
            return iae;
        }
        return new IllegalStateException("Kafka no respondio: " + causa.getMessage(), causa);
    }
}
