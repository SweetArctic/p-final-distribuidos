import time
import json
import random
import sys
import threading
import pika
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'sensor_data'
SENSOR_IDS = [f"sensor_{chr(ord('A') + i)}" for i in range(10)]

# RabbitMQ Configuration
RABBITMQ_HOST = 'rabbitmq'
SHUTDOWN_QUEUE = 'shutdown_sensor_queue'

# Shared state to manage sensor threads
sensor_threads = {}
stop_events = {}

def get_kafka_producer():
    """Establishes a connection to Kafka, with retries."""
    for _ in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8')
            )
            print("Conectado a Kafka exitosamente.")
            return producer
        except Exception as e:
            print(f"Error conectando a Kafka: {e}. Reintentando en 5 segundos...")
            time.sleep(5)
    print("No se pudo conectar a Kafka. Abortando.")
    sys.exit(1)

def run_sensor(sensor_id, producer, stop_event):
    """Generates and sends data for a single sensor until stopped."""
    while not stop_event.is_set():
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
        
        # Wait for a bit before the next reading
        time.sleep(6)
    print(f"--- Sensor {sensor_id} detenido. ---")

def shutdown_listener():
    """Listens to RabbitMQ for shutdown commands and stops the corresponding sensor thread."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=SHUTDOWN_QUEUE, durable=True)
    print("Escuchando comandos de apagado en RabbitMQ...")

    def callback(ch, method, properties, body):
        try:
            data = json.loads(body.decode('utf-8'))
            sensor_id_to_stop = data.get('sensor_id')
            
            if not sensor_id_to_stop:
                print(f"Error: No se encontró 'sensor_id' en el mensaje: {data}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            print(f"Recibido comando de apagado para: {sensor_id_to_stop}")
            if sensor_id_to_stop in stop_events:
                stop_events[sensor_id_to_stop].set()
                print(f"--- Sensor {sensor_id_to_stop} detenido. ---")
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                print(f"Error: Sensor ID '{sensor_id_to_stop}' no encontrado.")
                ch.basic_nack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            print(f"Error: No se pudo decodificar el mensaje JSON: {body}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            print(f"Error inesperado en el callback: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


    channel.basic_consume(queue=SHUTDOWN_QUEUE, on_message_callback=callback)
    channel.start_consuming()
    channel.close()
    connection.close()

def reactivate_listener():
    """Listens to RabbitMQ for reactivate commands and restarts the corresponding sensor thread."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue='reactivate_queue', durable=True)
    print("Escuchando comandos de reactivación en RabbitMQ...")

    def callback(ch, method, properties, body):
        try:
            data = json.loads(body.decode('utf-8'))
            sensor_id_to_reactivate = data.get('sensor_id')
            
            if not sensor_id_to_reactivate:
                print(f"Error: No se encontró 'sensor_id' en el mensaje: {data}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            print(f"Recibido comando de reactivación para: {sensor_id_to_reactivate}")
            if sensor_id_to_reactivate in stop_events:
                stop_events[sensor_id_to_reactivate].clear()
                thread = threading.Thread(target=run_sensor, args=(sensor_id_to_reactivate, get_kafka_producer(), stop_events[sensor_id_to_reactivate]))
                sensor_threads[sensor_id_to_reactivate] = thread
                thread.start()
                print(f"--- Sensor {sensor_id_to_reactivate} reactivado. ---")
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                print(f"Error: Sensor ID '{sensor_id_to_reactivate}' no encontrado.")
                ch.basic_nack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            print(f"Error: No se pudo decodificar el mensaje JSON: {body}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            print(f"Error inesperado en el callback: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


    channel.basic_consume(queue='reactivate_queue', on_message_callback=callback)
    channel.start_consuming()
    channel.close()
    connection.close()

if __name__ == "__main__":
    print("Iniciando sensor_producer...")
    producer = get_kafka_producer()

    # Start the shutdown listener in a separate thread
    shutdown_thread = threading.Thread(target=shutdown_listener, daemon=True)
    shutdown_thread.start()

    # Start the reactivate listener in a separate thread
    reactivate_thread = threading.Thread(target=reactivate_listener, daemon=True)
    reactivate_thread.start()

    # Start a thread for each sensor
    for sensor_id in SENSOR_IDS:
        stop_event = threading.Event()
        thread = threading.Thread(target=run_sensor, args=(sensor_id, producer, stop_event))
        
        stop_events[sensor_id] = stop_event
        sensor_threads[sensor_id] = thread
        
        thread.start()
        print(f"Sensor {sensor_id} iniciado.")

    try:
        # Keep the main thread alive to allow sensors to run
        while any(t.is_alive() for t in sensor_threads.values()):
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nCerrando todos los sensores por interrupción...")
        for stop_event in stop_events.values():
            stop_event.set()
    finally:
        print("Esperando que los hilos de los sensores terminen...")
        for thread in sensor_threads.values():
            thread.join()
        if producer:
            print("Cerrando productor de Kafka.")
            producer.close()
        print("Productor de sensores finalizado.")