"""
Productor Kafka - Simulación de reportes de casos COVID-19 en tiempo real.

Lee filas reales del CSV de casos positivos (datos.gov.co) y las envía, una a una,
al tópico 'covid_casos', como si cada fila fuera un nuevo reporte que llega
desde una secretaría de salud. Si no se indica archivo, genera eventos aleatorios.

Uso:
    python3 kafka_producer_covid.py [RUTA_CSV_LOCAL] [SEGUNDOS_ENTRE_EVENTOS]
Ejemplo:
    python3 kafka_producer_covid.py ~/datos/muestra_covid.csv 0.5
"""

import csv
import json
import random
import sys
import time
import unicodedata
from datetime import datetime

from kafka import KafkaProducer

TOPICO = "covid_casos"
SERVIDOR = "localhost:9092"
RUTA_CSV = sys.argv[1] if len(sys.argv) > 1 else None
PAUSA = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0


def normalizar(nombre):
    sin_tildes = "".join(c for c in unicodedata.normalize("NFD", nombre)
                         if unicodedata.category(c) != "Mn")
    return sin_tildes.strip().lower().replace(" ", "_")


CAMPOS = {
    "id_de_caso": "id_caso",
    "nombre_departamento": "departamento", "departamento_nom": "departamento",
    "nombre_municipio": "municipio", "ciudad_municipio_nom": "municipio",
    "edad": "edad",
    "sexo": "sexo",
    "tipo_de_contagio": "tipo_contagio", "fuente_tipo_contagio": "tipo_contagio",
    "ubicacion_del_caso": "ubicacion", "ubicacion": "ubicacion",
    "estado": "estado",
    "recuperado": "recuperado",
}


def evento_desde_fila(fila):
    evento = {}
    for clave, valor in fila.items():
        destino = CAMPOS.get(normalizar(clave))
        if destino:
            evento[destino] = (valor or "").strip()
    evento["departamento"] = evento.get("departamento", "").upper()
    evento["municipio"] = evento.get("municipio", "").upper()
    evento["sexo"] = evento.get("sexo", "").upper()
    evento["estado"] = evento.get("estado", "").capitalize()
    try:
        evento["edad"] = int(float(evento.get("edad") or 0))
    except ValueError:
        evento["edad"] = None
    return evento


def evento_aleatorio(contador):
    departamentos = ["BOGOTA", "ANTIOQUIA", "VALLE", "CUNDINAMARCA",
                     "ATLANTICO", "SANTANDER", "BOLIVAR", "META"]
    return {
        "id_caso": str(contador),
        "departamento": random.choice(departamentos),
        "municipio": "N/D",
        "edad": random.randint(0, 95),
        "sexo": random.choice(["F", "M"]),
        "tipo_contagio": random.choice(["Comunitaria", "Relacionado", "Importado"]),
        "ubicacion": random.choices(["Casa", "Hospital", "Hospital uci"], [85, 12, 3])[0],
        "estado": random.choices(["Leve", "Moderado", "Grave", "Fallecido"], [80, 12, 5, 3])[0],
        "recuperado": "Activo",
    }


productor = KafkaProducer(
    bootstrap_servers=[SERVIDOR],
    value_serializer=lambda d: json.dumps(d, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

print(f"Productor Kafka iniciado. Tópico: {TOPICO}")
print("Fuente:", RUTA_CSV if RUTA_CSV else "eventos aleatorios")
print("Presione Ctrl+C para detenerlo.\n")

enviados = 0
try:
    if RUTA_CSV:
        with open(RUTA_CSV, encoding="utf-8") as archivo:
            for fila in csv.DictReader(archivo):
                evento = evento_desde_fila(fila)
                evento["timestamp"] = int(time.time())
                # La clave (departamento) hace que los eventos del mismo
                # departamento caigan en la misma partición del tópico.
                productor.send(TOPICO, key=evento["departamento"] or "SIN_DATO", value=evento)
                enviados += 1
                print(f"[{datetime.now():%H:%M:%S}] #{enviados} {evento}")
                time.sleep(PAUSA)
    else:
        while True:
            evento = evento_aleatorio(enviados + 1)
            evento["timestamp"] = int(time.time())
            productor.send(TOPICO, key=evento["departamento"], value=evento)
            enviados += 1
            print(f"[{datetime.now():%H:%M:%S}] #{enviados} {evento}")
            time.sleep(PAUSA)
except KeyboardInterrupt:
    print("\nProductor detenido por el usuario.")
finally:
    productor.flush()
    productor.close()
    print(f"Total de eventos enviados: {enviados}")
