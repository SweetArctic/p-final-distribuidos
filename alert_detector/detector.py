import sys
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from collections import deque

KAFKA_BROKER = 'kafka:9092'
SOURCE_TOPIC = 'sensor_data'
CRITICAL_TOPIC = 'alerts_critical'
WARNING_TOPIC = 'alerts_warning'

RULE_CRITICAL_THRESHOLD = 90.0
RULE_WARNING_THRESHOLD = 75.0
RULE_WARNING_WINDOW = 3

print("Iniciando alert_detector...")

def connect_kafka():
    try:
        consumer = KafkaConsumer(
            SOURCE_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',
            group_id='alert_detectors',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Conectado a Kafka exitosamente.")
        return consumer, producer
    except Exception as e:
        print(f"Error conectando a Kafka: {e}")
        return None, None

consumer, producer = None, None
while not consumer or not producer:
    consumer, producer = connect_kafka()
    if not consumer or not producer:
        print("Reintentando conexión a Kafka en 5 segundos...")
        time.sleep(5)

sensor_states = {}

print("Escuchando mensajes de 'sensor_data'...")

try:
    for message in consumer:
        data = message.value
        sensor_id = data.get('sensor_id')
        vibration = data.get('vibration')
        
        if not sensor_id or vibration is None:
            continue

        if sensor_id not in sensor_states:
            sensor_states[sensor_id] = {
                'readings': deque(maxlen=RULE_WARNING_WINDOW),
                'last_alert': None 
            }
        
        state = sensor_states[sensor_id]
        state['readings'].append(vibration)
        
        alert_payload = {
            'sensor_id': sensor_id,
            'vibration': vibration,
            'timestamp': data.get('timestamp')
        }

        # Regla 1: Crítica (Tiene prioridad)
        if vibration > RULE_CRITICAL_THRESHOLD:
            if state['last_alert'] != 'CRITICAL':
                print(f"ALERTA CRÍTICA: Sensor {sensor_id} con {vibration}")
                producer.send(CRITICAL_TOPIC, value=alert_payload)
                state['last_alert'] = 'CRITICAL'
                
        # Regla 2: Advertencia (Solo si no es crítica)
        elif all(v > RULE_WARNING_THRESHOLD for v in state['readings']) and len(state['readings']) == RULE_WARNING_WINDOW:
            if state['last_alert'] != 'WARNING':
                print(f"ALERTA DE ADVERTENCIA: Sensor {sensor_id} con {vibration} (3 lecturas > {RULE_WARNING_THRESHOLD})")
                producer.send(WARNING_TOPIC, value=alert_payload)
                state['last_alert'] = 'WARNING'
                
        # Resetear estado si baja de la advertencia
        elif vibration < RULE_WARNING_THRESHOLD and state['last_alert']:
            print(f"Sensor {sensor_id} vuelve a estado normal.")
            state['last_alert'] = None

except KeyboardInterrupt:
    print("Cerrando detector de alertas.")
finally:
    if consumer:
        consumer.close()
    if producer:
        producer.close()