"""
Consumidor Spark Structured Streaming - Monitoreo de casos COVID-19 en tiempo real.

Lee el tópico 'covid_casos' de Kafka y produce dos salidas en consola:
  1. Estadísticas por ventana de 1 minuto y departamento (casos, edad promedio,
     casos graves y fallecidos).
  2. Alertas inmediatas de casos en estado Grave o Fallecido.

Uso:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        spark_streaming_covid.py
(Ajuste la versión del paquete a la versión de Spark instalada.)
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

TOPICO = "covid_casos"
SERVIDOR = "localhost:9092"
CHECKPOINTS = os.path.expanduser("~/checkpoints/covid")

spark = (
    SparkSession.builder
    .appName("StreamingCovidColombia_Estudiante3")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Esquema explícito del mensaje JSON (Structured Streaming exige esquema)
esquema_evento = StructType([
    StructField("id_caso", StringType()),
    StructField("departamento", StringType()),
    StructField("municipio", StringType()),
    StructField("edad", IntegerType()),
    StructField("sexo", StringType()),
    StructField("tipo_contagio", StringType()),
    StructField("ubicacion", StringType()),
    StructField("estado", StringType()),
    StructField("recuperado", StringType()),
    StructField("timestamp", LongType()),
])

flujo_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", SERVIDOR)
    .option("subscribe", TOPICO)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

eventos = (
    flujo_kafka
    .select(
        F.col("partition").alias("particion"),
        F.col("offset"),
        F.from_json(F.col("value").cast("string"), esquema_evento).alias("e"),
    )
    .select("particion", "offset", "e.*")
    .withColumn("fecha_hora", F.from_unixtime("timestamp").cast("timestamp"))
    .withColumn("es_grave", F.when(F.col("estado") == "Grave", 1).otherwise(0))
    .withColumn("es_fallecido", F.when(F.col("estado") == "Fallecido", 1).otherwise(0))
)

# 1. Estadísticas por ventana de tiempo y departamento
estadisticas = (
    eventos
    .withWatermark("fecha_hora", "2 minutes")
    .groupBy(F.window("fecha_hora", "1 minute"), "departamento")
    .agg(
        F.count("*").alias("casos"),
        F.round(F.avg("edad"), 1).alias("edad_promedio"),
        F.sum("es_grave").alias("graves"),
        F.sum("es_fallecido").alias("fallecidos"),
    )
    .select(
        F.date_format("window.start", "HH:mm:ss").alias("inicio"),
        F.date_format("window.end", "HH:mm:ss").alias("fin"),
        "departamento", "casos", "edad_promedio", "graves", "fallecidos",
    )
)

consulta_estadisticas = (
    estadisticas.writeStream
    .queryName("estadisticas_departamento")
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("numRows", 50)
    .option("checkpointLocation", f"{CHECKPOINTS}/estadisticas")
    .trigger(processingTime="10 seconds")
    .start()
)

# 2. Alertas: casos graves o fallecidos (sin agregación, modo append)
alertas = (
    eventos
    .filter(F.col("estado").isin("Grave", "Fallecido"))
    .select(
        F.date_format("fecha_hora", "HH:mm:ss").alias("hora"),
        "id_caso", "departamento", "municipio", "edad", "estado",
        "particion", "offset",
    )
)

consulta_alertas = (
    alertas.writeStream
    .queryName("alertas_graves")
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", f"{CHECKPOINTS}/alertas")
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Consumidor Spark Streaming leyendo el tópico '{TOPICO}'.")
print("Micro-lotes cada 10 segundos. Presione Ctrl+C para detenerlo.")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nConsumidor detenido por el usuario.")
finally:
    consulta_estadisticas.stop()
    consulta_alertas.stop()
    spark.stop()
