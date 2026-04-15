import time
import json
import random
from kafka import KafkaProducer

# Configuración del Productor
# Conectamos al servidor local en el puerto 9092
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_sensor_data():
    """Genera datos aleatorios simulando un sensor de temperatura y humedad."""
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "timestamp": int(time.time())
    }

print("--- Iniciando Productor de Datos de Sensores ---")
print("Enviando datos al topic 'sensor_data'. Presiona Ctrl+C para detener.")

try:
    while True:
        data = generate_sensor_data()
        # Enviamos el mensaje al topic 'sensor_data'
        producer.send('sensor_data', value=data)
        print(f"Dato enviado: {data}")
        time.sleep(1) # Espera 1 segundo entre envíos
except KeyboardInterrupt:
    print("\nProductor detenido por el usuario.")
finally:
    producer.close()
