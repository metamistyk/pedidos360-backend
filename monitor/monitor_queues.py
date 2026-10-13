#!/usr/bin/env python3
"""Monitor de colas RabbitMQ vía Management API (solo biblioteca estándar).

Uso:
  python monitor/monitor_queues.py --una-vez
  python monitor/monitor_queues.py --intervalo 15 --umbral-ready 10

Variables de entorno (no se imprimen nunca):
  RABBITMQ_MGMT_URL  (def. http://localhost:15672)
  RABBITMQ_USER      (def. guest)
  RABBITMQ_PASS      (def. guest)

Alertas:
  - cola *.dlq con Ready > 0
  - cualquier cola con Ready >= umbral
  - cola principal (no retry/dlq) con Consumers == 0 y Ready > 0
Los umbrales son de demostración; en operación real se ajustan al tráfico.
"""
import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime


def obtener_colas(base_url, user, password):
    req = urllib.request.Request(base_url.rstrip("/") + "/api/queues/%2F")
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    req.add_header("Authorization", "Basic " + token)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def evaluar(colas, umbral):
    filas, alertas = [], []
    for q in sorted(colas, key=lambda c: c["name"]):
        nombre = q["name"]
        ready = q.get("messages_ready", 0)
        unacked = q.get("messages_unacknowledged", 0)
        consumers = q.get("consumers", 0)
        filas.append((nombre, ready, unacked, consumers))
        if nombre.endswith(".dlq") and ready > 0:
            alertas.append(f"{nombre}: {ready} mensaje(s) fallido(s) en DLQ, requiere revisión")
        elif ready >= umbral:
            alertas.append(f"{nombre}: Ready={ready} >= {umbral}, pendientes acumulándose")
        if (not nombre.endswith((".dlq", ".retry.queue"))
                and consumers == 0 and ready > 0):
            alertas.append(f"{nombre}: sin consumidores y con Ready={ready} (¿backend detenido?)")
    return filas, alertas


def imprimir(filas, alertas):
    print(f"\n=== {datetime.now():%Y-%m-%d %H:%M:%S} ===")
    print(f"{'Cola':34} {'Ready':>6} {'Unacked':>8} {'Consumers':>10}")
    for n, r, u, c in filas:
        print(f"{n:34} {r:>6} {u:>8} {c:>10}")
    if alertas:
        for a in alertas:
            print("[ALERTA] " + a)
    else:
        print("[OK] sin alertas")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--una-vez", action="store_true", help="consulta una vez y termina")
    p.add_argument("--intervalo", type=int, default=15, help="segundos entre consultas")
    p.add_argument("--umbral-ready", type=int, default=10, help="Ready que dispara alerta")
    a = p.parse_args()

    url = os.environ.get("RABBITMQ_MGMT_URL", "http://localhost:15672")
    user = os.environ.get("RABBITMQ_USER", "guest")
    pwd = os.environ.get("RABBITMQ_PASS", "guest")

    try:
        while True:
            try:
                filas, alertas = evaluar(obtener_colas(url, user, pwd), a.umbral_ready)
                imprimir(filas, alertas)
            except urllib.error.HTTPError as e:
                print(f"[ERROR] la API respondió HTTP {e.code} (¿usuario/clave o URL?)")
            except urllib.error.URLError as e:
                print(f"[ERROR] no se pudo conectar a {url}: {e.reason}")
            if a.una_vez:
                break
            time.sleep(a.intervalo)
    except KeyboardInterrupt:
        print("\nMonitor detenido.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
