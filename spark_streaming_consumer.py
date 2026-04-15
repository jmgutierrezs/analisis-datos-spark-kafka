from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

# 1. Iniciar la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisSensoresRealTime") \
    .getOrCreate()

# Reducimos los mensajes informativos para ver solo lo importante
spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema de los datos que llegan (JSON)
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType())
])

# 3. Leer el flujo de datos desde Kafka
# Kafka actúa como la fuente (Source)
raw_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# 4. Transformar los datos
# Convertimos el valor binario a String y luego aplicamos el esquema JSON
json_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 5. Procesamiento: Agregación por ventana de 1 minuto
# Calculamos el promedio de temperatura y humedad por cada sensor
stats_df = json_df \
    .groupBy(
        window(col("timestamp"), "1 minute"), 
        "sensor_id"
    ) \
    .agg({
        "temperature": "avg", 
        "humidity": "avg"
    })

# 6. Salida: Mostrar los resultados en la consola
# El modo 'complete' muestra toda la tabla actualizada
query = stats_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

print("--- Iniciando Procesamiento en Spark ---")
query.awaitTermination()
