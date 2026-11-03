# =============================================================================
# Administracion del cluster Kafka de Pedidos360 desde la linea de comandos.
# Para PowerShell (Windows). En Linux / AWS usar scripts/kafka.sh.
#
# Usa las herramientas que ya traen los contenedores de Kafka (docker exec), asi
# que funciona igual en local y en la EC2 de AWS: solo requiere que el cluster
# este arriba (docker compose -f docker-compose.kafka.yml up -d).
#
# Uso:  .\scripts\kafka.ps1 <accion> [argumentos]
#
#   topics-crear                       crea los 3 topicos (3 particiones, RF 3)
#   topics-listar                      lista los topicos
#   topics-describir [topico]          particiones, lider, replicas e ISR
#   topics-retencion <topico> <ms>     cambia la retencion en caliente
#   mensajes <topico> [max]            lee mensajes (clave, particion, offset)
#   grupos-listar                      lista los consumer groups
#   grupos-lag <grupo>                 offsets, fin del log y lag por particion
#   grupos-reset <grupo> <topico> [-Ejecutar]
#                                      vuelve al inicio del topico (sin
#                                      -Ejecutar solo muestra que haria; el
#                                      backend debe estar detenido)
#
# Variables de entorno opcionales:
#   KAFKA_CONTENEDOR        contenedor donde se ejecuta (pedidos360-kafka1)
#   KAFKA_BOOTSTRAP_INTERNO brokers vistos desde la red de Docker
#   KAFKA_RETENCION_MS      retencion al crear topicos (604800000 = 7 dias)
# =============================================================================
param(
    [Parameter(Position = 0)][string]$Accion,
    [Parameter(Position = 1)][string]$Arg1,
    [Parameter(Position = 2)][string]$Arg2,
    [switch]$Ejecutar
)

$ErrorActionPreference = "Stop"

$Contenedor = if ($env:KAFKA_CONTENEDOR) { $env:KAFKA_CONTENEDOR } else { "pedidos360-kafka1" }
$Bootstrap  = if ($env:KAFKA_BOOTSTRAP_INTERNO) { $env:KAFKA_BOOTSTRAP_INTERNO } else { "kafka1:29092,kafka2:29092,kafka3:29092" }
$RetencionMs = if ($env:KAFKA_RETENCION_MS) { $env:KAFKA_RETENCION_MS } else { "604800000" }
$Particiones = 3
$Replicas = 3
$Topicos = @("pedidos.eventos", "inventario.eventos", "pedidos360.errores")

# Ejecuta un comando dentro del contenedor de Kafka.
function K {
    docker exec $Contenedor @args
}

function Uso {
    Get-Content $PSCommandPath | Select-Object -Skip 1 -First 25 | ForEach-Object { $_ -replace '^# ?', '' }
    exit 1
}

function Requiere($valor, $uso) {
    if (-not $valor) {
        Write-Host "Faltan argumentos. Uso: $uso"
        exit 1
    }
}

if (-not $Accion) { Uso }

switch ($Accion) {

    "topics-crear" {
        foreach ($t in $Topicos) {
            K kafka-topics --bootstrap-server $Bootstrap --create --if-not-exists `
                --topic $t --partitions $Particiones --replication-factor $Replicas `
                --config "retention.ms=$RetencionMs" --config "min.insync.replicas=2"
        }
        Write-Host "Topicos listos:"
        K kafka-topics --bootstrap-server $Bootstrap --list
    }

    "topics-listar" {
        K kafka-topics --bootstrap-server $Bootstrap --list
    }

    "topics-describir" {
        if ($Arg1) {
            K kafka-topics --bootstrap-server $Bootstrap --describe --topic $Arg1
        } else {
            K kafka-topics --bootstrap-server $Bootstrap --describe
        }
    }

    "topics-retencion" {
        Requiere ($Arg1 -and $Arg2) "topics-retencion <topico> <ms>"
        K kafka-configs --bootstrap-server $Bootstrap --entity-type topics `
            --entity-name $Arg1 --alter --add-config "retention.ms=$Arg2"
        K kafka-configs --bootstrap-server $Bootstrap --entity-type topics `
            --entity-name $Arg1 --describe
    }

    "mensajes" {
        Requiere $Arg1 "mensajes <topico> [max]"
        $max = if ($Arg2) { $Arg2 } else { "10" }
        K kafka-console-consumer --bootstrap-server $Bootstrap --topic $Arg1 `
            --from-beginning --max-messages $max --timeout-ms 5000 `
            --property print.key=true --property print.partition=true `
            --property print.offset=true
    }

    "grupos-listar" {
        K kafka-consumer-groups --bootstrap-server $Bootstrap --list
    }

    "grupos-lag" {
        Requiere $Arg1 "grupos-lag <grupo>"
        K kafka-consumer-groups --bootstrap-server $Bootstrap --describe --group $Arg1
    }

    "grupos-reset" {
        Requiere ($Arg1 -and $Arg2) "grupos-reset <grupo> <topico> [-Ejecutar]"
        if ($Ejecutar) {
            K kafka-consumer-groups --bootstrap-server $Bootstrap --group $Arg1 `
                --topic $Arg2 --reset-offsets --to-earliest --execute
        } else {
            Write-Host "Simulacion (agrega -Ejecutar para aplicar; el backend debe estar detenido):"
            K kafka-consumer-groups --bootstrap-server $Bootstrap --group $Arg1 `
                --topic $Arg2 --reset-offsets --to-earliest --dry-run
        }
    }

    default {
        Write-Host "Accion desconocida: $Accion"
        Uso
    }
}
