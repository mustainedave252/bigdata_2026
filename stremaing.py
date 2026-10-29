
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

# Crear sesión de Spark
spark = (
    SparkSession.builder
    .appName("NetflixStructuredStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Definir la estructura de los eventos JSON
esquema = StructType([
    StructField("evento", StringType(), True),
    StructField("show_id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("release_year", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("event_time", StringType(), True)
])

# Leer eventos desde Kafka
eventos_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "netflix_events")
    .option("startingOffsets", "latest")
    .load()
)

# Convertir el mensaje Kafka de bytes a JSON
eventos_json = eventos_kafka.select(
    from_json(
        col("value").cast("string"),
        esquema
    ).alias("datos")
)

# Extraer campos del JSON
eventos = eventos_json.select("datos.*")

# Filtrar eventos válidos
eventos_validos = eventos.filter(
    col("title").isNotNull()
    & col("type").isin("Movie", "TV Show")
)

# Agrupar eventos por tipo de contenido
conteo_por_tipo = (
    eventos_validos
    .groupBy("type")
    .count()
)

# Mostrar resultados continuamente en consola
consulta = (
    conteo_por_tipo.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="5 seconds")
    .start()
)

print("Spark Structured Streaming está escuchando el topic netflix_events.")

consulta.awaitTermination()
