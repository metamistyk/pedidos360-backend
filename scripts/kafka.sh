#!/usr/bin/env bash
# =============================================================================
# Administracion del cluster Kafka de Pedidos360 desde la linea de comandos.
# Para Linux / AWS (EC2) / Git Bash / WSL. En PowerShell usar scripts/kafka.ps1.
#
# Usa las herramientas que ya traen los contenedores de Kafka (docker exec), asi
# que funciona igual en local y en la EC2 de AWS: solo requiere que el cluster
# este arriba (docker compose -f docker-compose.kafka.yml up -d).
#
# Uso:  ./scripts/kafka.sh <accion> [argumentos]
#
#   topics-crear                       crea los 3 topicos (3 particiones, RF 3)
#   topics-listar                      lista los topicos
#   topics-describir [topico]          particiones, lider, replicas e ISR
#   topics-retencion <topico> <ms>     cambia la retencion en caliente
#   mensajes <topico> [max]            lee mensajes (clave, particion, offset)
#   grupos-listar                      lista los consumer groups
#   grupos-lag <grupo>                 offsets, fin del log y lag por particion
#   grupos-reset <grupo> <topico> [--ejecutar]
#                                      vuelve al inicio del topico (sin
#                                      --ejecutar solo muestra que haria; el
#                                      backend debe estar detenido)
#
# Variables opcionales:
#   KAFKA_CONTENEDOR       contenedor donde se ejecuta (pedidos360-kafka1)
#   KAFKA_BOOTSTRAP_INTERNO brokers vistos desde la red de Docker
#   KAFKA_RETENCION_MS     retencion al crear topicos (604800000 = 7 dias)
# =============================================================================
set -euo pipefail

CONTENEDOR="${KAFKA_CONTENEDOR:-pedidos360-kafka1}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNO:-kafka1:29092,kafka2:29092,kafka3:29092}"
RETENCION_MS="${KAFKA_RETENCION_MS:-604800000}"
PARTICIONES=3
REPLICAS=3
TOPICOS=(pedidos.eventos inventario.eventos pedidos360.errores)

# Ejecuta un comando dentro del contenedor de Kafka.
k() { docker exec "$CONTENEDOR" "$@"; }

uso() {
  sed -n '3,27p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

requiere() {
  # requiere <cantidad> <mensaje>: valida que haya argumentos suficientes
  if [ "$1" -lt "$2" ]; then
    echo "Faltan argumentos. Uso: $3" >&2
    exit 1
  fi
}

accion="${1:-}"
[ -n "$accion" ] || uso
shift || true

case "$accion" in

  topics-crear)
    for t in "${TOPICOS[@]}"; do
      k kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$t" --partitions "$PARTICIONES" --replication-factor "$REPLICAS" \
        --config "retention.ms=$RETENCION_MS" --config min.insync.replicas=2
    done
    echo "Topicos listos:"
    k kafka-topics --bootstrap-server "$BOOTSTRAP" --list
    ;;

  topics-listar)
    k kafka-topics --bootstrap-server "$BOOTSTRAP" --list
    ;;

  topics-describir)
    if [ $# -ge 1 ]; then
      k kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$1"
    else
      k kafka-topics --bootstrap-server "$BOOTSTRAP" --describe
    fi
    ;;

  topics-retencion)
    requiere $# 2 "topics-retencion <topico> <ms>"
    k kafka-configs --bootstrap-server "$BOOTSTRAP" --entity-type topics \
      --entity-name "$1" --alter --add-config "retention.ms=$2"
    k kafka-configs --bootstrap-server "$BOOTSTRAP" --entity-type topics \
      --entity-name "$1" --describe
    ;;

  mensajes)
    requiere $# 1 "mensajes <topico> [max]"
    max="${2:-10}"
    k kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --topic "$1" \
      --from-beginning --max-messages "$max" --timeout-ms 5000 \
      --property print.key=true --property print.partition=true \
      --property print.offset=true || true
    ;;

  grupos-listar)
    k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --list
    ;;

  grupos-lag)
    requiere $# 1 "grupos-lag <grupo>"
    k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --describe --group "$1"
    ;;

  grupos-reset)
    requiere $# 2 "grupos-reset <grupo> <topico> [--ejecutar]"
    if [ "${3:-}" = "--ejecutar" ]; then
      k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --group "$1" \
        --topic "$2" --reset-offsets --to-earliest --execute
    else
      echo "Simulacion (agrega --ejecutar para aplicar; el backend debe estar detenido):"
      k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" --group "$1" \
        --topic "$2" --reset-offsets --to-earliest --dry-run
    fi
    ;;

  *)
    echo "Accion desconocida: $accion" >&2
    uso
    ;;
esac
