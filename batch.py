
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, coalesce, to_date, year, when,
    count, avg, min, max, split, explode
)

# Crear sesión de Spark
spark = (
    SparkSession.builder
    .appName("NetflixBatchProcessing")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Ruta del conjunto de datos
ruta_csv = "/home/destino/NetFlix.csv"
ruta_resultados = "/home/destino/proyecto_netflix/resultados"

# Cargar el archivo CSV
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ruta_csv)
)

print("\n========== DATOS ORIGINALES ==========")
print("Número de registros:", df.count())
print("Número de columnas:", len(df.columns))
df.printSchema()

# Eliminar duplicados por identificador
df = df.dropDuplicates(["show_id"])

# Limpiar espacios en columnas de texto
columnas_texto = [
    "show_id", "type", "title", "director", "cast",
    "country", "rating", "duration", "listed_in",
    "description", "date_added"
]

for nombre in columnas_texto:
    if nombre in df.columns:
        df = df.withColumn(
            nombre,
            trim(col(nombre).cast("string"))
        )

# Reemplazar cadenas vacías por valores nulos
for nombre in columnas_texto:
    if nombre in df.columns:
        df = df.withColumn(
            nombre,
            when(col(nombre) == "", None).otherwise(col(nombre))
        )

# Completar valores nulos en campos descriptivos
df = df.fillna({
    "director": "Desconocido",
    "cast": "No especificado",
    "country": "No especificado",
    "rating": "Sin clasificación",
    "listed_in": "Sin categoría",
    "description": "Sin descripción"
})

# Normalizar fechas con diferentes formatos
fecha = coalesce(
    to_date(col("date_added"), "d-MMM-yy"),
    to_date(col("date_added"), "MMMM d, yyyy"),
    to_date(col("date_added"), "MMM d, yyyy")
)

df = df.withColumn("date_added", fecha)

# Crear variables derivadas
df = df.withColumn(
    "duration_unit",
    when(col("type") == "Movie", "minutos")
    .when(col("type") == "TV Show", "temporadas")
    .otherwise("no especificado")
)

df = df.withColumn(
    "decade",
    (col("release_year") / 10).cast("int") * 10
)

print("\n========== DATOS LIMPIOS ==========")
print("Registros procesados:", df.count())
df.show(10, truncate=False)

# Verificar valores nulos
print("\n========== VALORES NULOS ==========")

for nombre in df.columns:
    cantidad = df.filter(col(nombre).isNull()).count()
    print(f"{nombre}: {cantidad}")

# Distribución por tipo de contenido
print("\n========== TIPOS DE CONTENIDO ==========")
df.groupBy("type").count().orderBy(
    col("count").desc()
).show()

# Distribución por año de lanzamiento
print("\n========== AÑOS DE LANZAMIENTO ==========")
df.groupBy("release_year").count().orderBy(
    col("count").desc()
).show(10)

# Distribución por clasificación
print("\n========== CLASIFICACIONES ==========")
df.groupBy("rating").count().orderBy(
    col("count").desc()
).show()

# Países de producción
print("\n========== PRINCIPALES PAISES ==========")
paises = (
    df.withColumn("country_item", explode(split(col("country"), ",")))
    .withColumn("country_item", trim(col("country_item")))
    .filter(
        col("country_item").isNotNull()
        & (col("country_item") != "")
        & (col("country_item") != "No especificado")
    )
)

paises.groupBy("country_item").count().orderBy(
    col("count").desc()
).show(10, truncate=False)

# Géneros o categorías
print("\n========== PRINCIPALES GENEROS ==========")
generos = (
    df.withColumn("genre_item", explode(split(col("listed_in"), ",")))
    .withColumn("genre_item", trim(col("genre_item")))
)

generos.groupBy("genre_item").count().orderBy(
    col("count").desc()
).show(10, truncate=False)

# Estadísticas de duración
print("\n========== DURACION DE PELICULAS ==========")
peliculas = df.filter(
    (col("type") == "Movie")
    & col("duration").rlike("^[0-9]+")
).withColumn(
    "duration_value",
    split(col("duration"), " ").getItem(0).cast("double")
)

peliculas.select(
    avg("duration_value").alias("promedio_minutos"),
    min("duration_value").alias("minimo_minutos"),
    max("duration_value").alias("maximo_minutos")
).show()

print("\n========== DURACION DE SERIES ==========")
series = df.filter(
    (col("type") == "TV Show")
    & col("duration").rlike("^[0-9]+")
).withColumn(
    "duration_value",
    split(col("duration"), " ").getItem(0).cast("double")
)

series.select(
    avg("duration_value").alias("promedio_temporadas"),
    min("duration_value").alias("minimo_temporadas"),
    max("duration_value").alias("maximo_temporadas")
).show()

# Guardar los datos procesados
df.write.mode("overwrite").parquet(
    f"{ruta_resultados}/netflix_limpio_parquet"
)

df.write.mode("overwrite").option(
    "header", True
).csv(f"{ruta_resultados}/netflix_limpio_csv")

print("\nResultados guardados correctamente.")
print("Directorio:", ruta_resultados)

spark.stop()
