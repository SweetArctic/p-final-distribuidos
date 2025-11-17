import time
import json
import random
import sys
from kafka import KafkaProducer

KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'sensor_data'
SENSOR_IDS = [f"sensor_{chr(ord('A') + i)}" for i in range(10)]

print("Iniciando sensor_producer...")

producer = None
for _ in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        print("Conectado a Kafka exitosamente.")
        break
    except Exception as e:
        print(f"Error conectando a Kafka: {e}. Reintentando en 5 segundos...")
        time.sleep(5)

if not producer:
    print("No se pudo conectar a Kafka. Abortando.")
    sys.exit(1)

try:
    while True:
        print("\n--- Nuevo ciclo de envío ---")
        for sensor_id in SENSOR_IDS:
            
            chance = random.random()
            
            if chance < 0.40:
                vibration = round(random.uniform(10.0, 74.9), 2)
            elif chance < 0.98:
                vibration = round(random.uniform(75.0, 90.0), 2)
            else:
                vibration = round(random.uniform(90.1, 105.0), 2)
            
            message = {
                'sensor_id': sensor_id,
                'vibration': vibration,
                'timestamp': time.time()
            }
            
            print(f"Enviando: {message}")
            producer.send(KAFKA_TOPIC, key=sensor_id, value=message)
        
        producer.flush()
        
        print("--- Ciclo completado. Esperando 6 segundos. ---")
        time.sleep(6)

except KeyboardInterrupt:
    print("Cerrando productor.")
finally:
    if producer:
        producer.close()