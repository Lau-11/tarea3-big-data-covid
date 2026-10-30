# Procesamiento de datos con Apache Spark y Kafka — Casos positivos de COVID-19 en Colombia

## Autor

**Laura Molano** — Estudiante 3 (Evaluador)
Universidad Nacional Abierta y a Distancia - UNAD
Curso: Big Data
Tarea 3: Procesamiento de datos con Apache Spark
Componente teórico a cargo: **Estructuras tabulares en Spark (DataFrames)**

## Descripción

Este repositorio implementa dos soluciones de procesamiento sobre los registros oficiales de casos positivos de COVID-19 en Colombia:

1. **Procesamiento batch** con PySpark y HDFS: carga, limpieza, análisis exploratorio (EDA), agregaciones, Spark SQL y almacenamiento de resultados.
2. **Procesamiento en tiempo real** con Apache Kafka y Spark Structured Streaming: un productor publica reportes de casos y un consumidor calcula estadísticas por ventana de tiempo y emite alertas de casos graves.

Además, el script batch incluye una comparación práctica **DataFrame vs. RDD** y muestra el **plan físico generado por el optimizador Catalyst**, como evidencia del tema teórico del Estudiante 3.

## Problema que se aborda

Durante la pandemia, el Instituto Nacional de Salud (INS) publicó cada caso confirmado de COVID-19 como un registro individual. El conjunto acumulado supera los seis millones de filas, un volumen que dificulta el análisis con herramientas tradicionales como hojas de cálculo. El proyecto busca responder, con procesamiento distribuido:

- ¿Cómo evolucionaron los contagios mes a mes (curva epidémica)?
- ¿Qué departamentos y municipios concentraron más casos?
- ¿Cómo cambia la letalidad según el grupo de edad?
- ¿Cuánto tarda, en promedio, un paciente en recuperarse?
- ¿Cómo se podrían vigilar en tiempo real los casos graves que reporta cada territorio?

## Dataset

**Casos positivos de COVID-19 en Colombia** — Instituto Nacional de Salud, publicado en el Portal de Datos Abiertos del Gobierno de Colombia.

Enlace oficial: <https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia-/gt2j-8ykr>

Cada fila es un caso confirmado. Variables principales:

| Variable (nombre en el CSV) | Uso en el análisis |
|---|---|
| fecha reporte web, Fecha de diagnóstico | Curva epidémica por mes |
| ID de caso | Detección de duplicados |
| Nombre departamento, Nombre municipio | Agregaciones territoriales |
| Edad, Unidad de medida de edad | Edad en años y grupos etarios |
| Sexo | Distribución por sexo |
| Tipo de contagio, Ubicación del caso | Perfil del caso |
| Estado, Recuperado | Gravedad y letalidad |
| Fecha de recuperación | Tiempo de recuperación |

El archivo original (más de 1 GB) **no se incluye** en este repositorio por su tamaño. Se descarga desde el portal con *Exportar → CSV*.

## Tecnologías utilizadas

- Ubuntu Linux
- Java 11
- Apache Hadoop y HDFS
- Apache Spark 3.5.x / PySpark
- Apache Kafka 3.x y ZooKeeper
- Python 3 (kafka-python, pandas, matplotlib)

## Estructura del repositorio

```
tarea3-big-data-covid/
├── README.md
├── .gitignore
├── requirements.txt
├── scripts/
│   ├── procesamiento_batch_covid.py   # Batch: limpieza, EDA, agregaciones, SQL, RDD vs DataFrame
│   ├── generar_graficos.py            # Visualizaciones a partir de los resultados
│   ├── kafka_producer_covid.py        # Productor Kafka (tópico covid_casos)
│   └── spark_streaming_covid.py       # Consumidor Spark Structured Streaming
├── notebooks/
│   └── tarea3_covid_colab.ipynb       # Ejecución completa en Google Colab
└── graficos/                          # Imágenes generadas con el dataset real
```

## Arquitectura

```
                    PROCESAMIENTO BATCH
  CSV datos.gov.co ──► HDFS ──► PySpark (DataFrames + Spark SQL) ──► HDFS (Parquet / CSV)
                                                                   └─► CSV locales ──► Matplotlib (PNG)

                    PROCESAMIENTO EN TIEMPO REAL
  kafka_producer_covid.py ──► Kafka (tópico covid_casos) ──► Spark Structured Streaming
                                                                ├─► Estadísticas por ventana de 1 min
                                                                └─► Alertas de casos Grave / Fallecido
```

## 1. Procesamiento batch

### Preparación de los datos en HDFS

```bash
# Iniciar Hadoop
start-dfs.sh

# Crear carpeta y subir el CSV descargado del portal
hdfs dfs -mkdir -p /Tarea3/laura/datos
hdfs dfs -put ~/Descargas/Casos_positivos_de_COVID-19_en_Colombia.csv \
    /Tarea3/laura/datos/casos_covid_colombia.csv
hdfs dfs -ls /Tarea3/laura/datos
```

### Ejecución

```bash
spark-submit scripts/procesamiento_batch_covid.py
# o indicando rutas propias:
spark-submit scripts/procesamiento_batch_covid.py \
    hdfs://localhost:9000/Tarea3/laura/datos/casos_covid_colombia.csv \
    hdfs://localhost:9000/Tarea3/laura/resultados
```

### Qué hace el script

| Paso | Detalle |
|---|---|
| 1. Carga | Lee el CSV desde HDFS como DataFrame (todo como texto para controlar el tipado). |
| 2. Estandarización | Renombra columnas a un formato único; acepta los nombres del CSV y los de la API. |
| 3. Esquema | Convierte tipos (enteros, decimales, fechas en varios formatos) y documenta el esquema objetivo con `StructType`. |
| 4. Calidad | Cuenta nulos por columna y duplicados por `id_caso`. |
| 5. Limpieza | Elimina duplicados y registros inválidos, convierte la edad a años (meses y días), crea grupos de edad, marca fallecidos y calcula días hasta la recuperación. |
| 6. EDA | Estadísticas de edad, mediana aproximada y distribuciones por sexo, estado, recuperación, tipo de contagio y ubicación. |
| 7. Agregaciones | Casos, fallecidos y letalidad por departamento; casos por mes; letalidad por grupo de edad; tabla dinámica edad × sexo; tiempo de recuperación. |
| 8. Spark SQL | Vista temporal `casos_covid`, ranking de municipios por departamento con `ROW_NUMBER()` y acumulado mensual con funciones de ventana. |
| 9. DataFrame vs RDD | Misma agregación con ambas APIs, comparación de tiempos y plan físico de Catalyst (`explain`). |
| 10. Almacenamiento | Datos limpios en Parquet particionado por año y resultados en CSV en HDFS. |

### Resultados en HDFS

```bash
hdfs dfs -ls /Tarea3/laura/resultados
hdfs dfs -cat /Tarea3/laura/resultados/casos_por_departamento/part-*.csv | head
```

### Gráficos

```bash
python3 scripts/generar_graficos.py
cp ~/graficos_covid/salidas/*.png graficos/
```

Se generan cuatro imágenes: casos por mes, top 10 departamentos, letalidad por grupo de edad y casos por edad y sexo.

## 2. Procesamiento en tiempo real (Kafka + Spark Streaming)

### Instalación de dependencias

```bash
pip3 install -r requirements.txt
```

### Iniciar ZooKeeper y Kafka (cada uno en una terminal)

```bash
cd /opt/kafka   # ajustar a la ruta de instalación
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

### Crear el tópico

```bash
bin/kafka-topics.sh --create --topic covid_casos \
    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --describe --topic covid_casos --bootstrap-server localhost:9092
```

### Ejecutar el productor (terminal 3)

Se usa una muestra del CSV real para simular la llegada de reportes:

```bash
head -n 5001 ~/Descargas/Casos_positivos_de_COVID-19_en_Colombia.csv > ~/muestra_covid.csv
python3 scripts/kafka_producer_covid.py ~/muestra_covid.csv 0.5
```

Sin argumentos, el productor genera eventos aleatorios con la misma estructura.

### Ejecutar el consumidor (terminal 4)

```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    scripts/spark_streaming_covid.py
```

> Ajuste `3.5.3` a la versión de Spark instalada (`spark-submit --version`).

El consumidor ejecuta dos consultas en micro-lotes de 10 segundos:

1. **Estadísticas por departamento** en ventanas de 1 minuto: número de casos, edad promedio, casos graves y fallecidos (modo `update`, con *watermark* de 2 minutos).
2. **Alertas**: lista inmediata de casos en estado *Grave* o *Fallecido*, con la partición y el *offset* de Kafka de donde se leyó cada evento (modo `append`).

Los *checkpoints* se guardan en `~/checkpoints/covid`, lo que permite reanudar el procesamiento sin perder ni repetir mensajes tras una falla.

## Ejecución alternativa en Google Colab

El cuaderno `notebooks/tarea3_covid_colab.ipynb` ejecuta todo el proyecto en Google Colab, sin instalar nada en el computador: instala Java y PySpark, descarga el dataset desde datos.gov.co, corre el procesamiento batch, genera los gráficos y levanta Kafka (ZooKeeper + broker) con el productor y el consumidor de Spark Structured Streaming. En Colab se usa el disco local del entorno en lugar de HDFS.

Para abrirlo: en <https://colab.research.google.com> elegir **Subir** y seleccionar el archivo `.ipynb`, o abrirlo directamente desde GitHub con la pestaña **GitHub** de Colab.

## Conceptos aplicados del componente teórico (Estudiante 3)

- **DataFrame con esquema estructurado**: los datos se organizan en columnas con nombre y tipo; el esquema se valida con `printSchema()` y se documenta con `StructType`.
- **Ventajas frente a RDD**: la API declarativa (`groupBy`, `agg`, SQL) es más corta y legible, y en la prueba incluida el DataFrame resuelve la misma agregación en menos tiempo que el RDD con funciones Python.
- **Catalyst y Tungsten**: `explain(mode="formatted")` muestra el plan físico optimizado (agregación parcial antes del intercambio de datos), y Tungsten ejecuta sobre memoria binaria compacta.

## Validación

El script batch se probó con una muestra de prueba que reproduce el formato del CSV del portal (dos formatos de fecha, mayúsculas y minúsculas mezcladas y un ID duplicado): los diez pasos se completaron sin errores. La lógica del consumidor (esquema, ventanas y alertas) también se verificó con una fuente de prueba. Los resultados y gráficos de la carpeta `graficos/` corresponden a la ejecución con el dataset real.

## Referencias

- Instituto Nacional de Salud. (2023). *Casos positivos de COVID-19 en Colombia* [Conjunto de datos]. Portal de Datos Abiertos. https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia-/gt2j-8ykr
- Apache Software Foundation. (2024). *Spark SQL, DataFrames and Datasets Guide*. https://spark.apache.org/docs/latest/sql-programming-guide.html
- Apache Software Foundation. (2024). *Structured Streaming + Kafka Integration Guide*. https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
- Armbrust, M., Xin, R. S., Lian, C., Huai, Y., Liu, D., Bradley, J. K., ... Zaharia, M. (2015). Spark SQL: Relational data processing in Spark. *Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data*, 1383–1394. https://doi.org/10.1145/2723372.2742797
