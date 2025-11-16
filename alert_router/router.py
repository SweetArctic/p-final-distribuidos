import sys
import json
import time
import pika
import random
import uuid
from kafka import KafkaConsumer

KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPICS = ['alerts_critical', 'alerts_warning']
RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_EXCHANGE = 'human_alerts'

print("Iniciando alert_router...")

def connect_kafka():
    try:
        consumer = KafkaConsumer(
            *KAFKA_TOPICS,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',
            group_id='alert_routers_group',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print("Router: Conectado a Kafka.")
        return consumer
    except Exception as e:
        print(f"Router: Error conectando a Kafka: {e}. Reintentando...")
        return None

def connect_rabbitmq():
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=5672, credentials=pika.PlainCredentials('user', 'password'))
        )
        channel = connection.channel()
        # Declara el Fanout Exchange
        channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type='fanout')
        print("Router: Conectado a RabbitMQ y exchange 'human_alerts' declarado.")
        return connection, channel
    except Exception as e:
        print(f"Router: Error conectando a RabbitMQ: {e}. Reintentando...")
        return None, None

kafka_consumer = None
while not kafka_consumer:
    kafka_consumer = connect_kafka()
    if not kafka_consumer:
        time.sleep(5)

rmq_connection, rmq_channel = None, None
while not rmq_channel:
    rmq_connection, rmq_channel = connect_rabbitmq()
    if not rmq_channel:
        time.sleep(5)

print("Router: Escuchando alertas de Kafka para enrutar a RabbitMQ...")

try:
    for message in kafka_consumer:
        alert_data = message.value
        alert_type = message.topic.split('_')[-1].upper() # CRITICAL o WARNING
        
        options = []
        if alert_type == 'CRITICAL':
            options = ["APAGADO_INMEDIATO", "IGNORAR_10_MINUTOS"] 
        elif alert_type == 'WARNING':
            options = ["PROGRAMAR_MANTENIMIENTO_AHORA", "RECONOCER_Y_ESPERAR_24H"] 

        if not options:
            continue

        message_to_operator = {
            "alert_id": str(uuid.uuid4()),
            "sensor_id": alert_data.get("sensor_id"),
            "type": alert_type,
            "vibration": alert_data.get("vibration"),
            "timestamp": alert_data.get("timestamp"),
            "options": options
        }
        
        try:
            rmq_channel.basic_publish(
                exchange=RABBITMQ_EXCHANGE,
                routing_key='', # Ignorado en fanout
                body=json.dumps(message_to_operator),
                properties=pika.BasicProperties(
                    content_type='application/json',
                    delivery_mode=2, # Hacer mensajes persistentes
                )
            )
            print(f"Router: Alerta {message_to_operator['alert_id']} (Sensor {message_to_operator['sensor_id']}) enviada a RabbitMQ.")
            
        except pika.exceptions.StreamLostError:
            print("Router: Conexión con RabbitMQ perdida. Reconectando...")
            while not rmq_channel:
                rmq_connection, rmq_channel = connect_rabbitmq()
                if not rmq_channel:
                    time.sleep(5)

except KeyboardInterrupt:
    print("Cerrando el router.")
finally:
    if kafka_consumer:
        kafka_consumer.close()
    if rmq_connection:
        rmq_connection.close()