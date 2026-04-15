# analisis-datos-spark-kafka
# Procesamiento de Datos de Sensores (Kafka + Spark)

Este proyecto realiza el procesamiento en batch/streaming de datos de sensores.

## Requisitos
* Apache Kafka 3.9.2
* Apache Spark
* Python 3.x con las librerías `pyspark` y `kafka-python`

## Instrucciones de Ejecución
1. Iniciar Zookeeper: `./bin/zookeeper-server-start.sh config/zookeeper.properties`
2. Iniciar Kafka: `./bin/kafka-server-start.sh config/server.properties`
3. Crear el topic: `sensor_data`
4. Ejecutar el productor: `python3 kafka_producer.py`
5. Ejecutar el consumidor de Spark: `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_streaming_consumer.py`
