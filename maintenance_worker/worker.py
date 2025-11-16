import sys
import json
import time
import pika

RABBITMQ_HOST = 'rabbitmq'
QUEUE_NAME = 'maintenance_queue'

print("Iniciando maintenance_worker...")

def connect_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, port=5672, credentials=pika.PlainCredentials('user', 'password'))
            )
            channel = connection.channel()
            
            # Asegurarse de que la cola exista (sea durable)
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            
            # Configurar prefetch_count=1 para que solo tome un mensaje a la vez
            channel.basic_qos(prefetch_count=1)
            
            print(f"Maintenance Worker: Conectado. Esperando tareas en '{QUEUE_NAME}'...")
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Maintenance Worker: Error conectando a RabbitMQ: {e}. Reintentando en 5s...")
            time.sleep(5)

def on_message_callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode('utf-8'))
        alert_id = data.get('alert_id')
        action = data.get('action')
        
        print("\n--- ¡TAREA DE MANTENIMIENTO RECIBIDA! ---")
        print(f"  Alerta ID: {alert_id}")
        print(f"  Acción: {action}")
        
        # Simular el trabajo (ej. crear una orden de trabajo)
        print(f"  ... Creando orden de trabajo en sistema de mantenimiento ...")
        time.sleep(2) 
        print(f"  ... Orden de trabajo creada exitosamente ...")
        
        # Confirmar (ack) que el mensaje fue procesado
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"--- Tarea {alert_id} completada. Esperando nuevas tareas. ---")
        
    except json.JSONDecodeError:
        print(f"Error: No se pudo decodificar el mensaje: {body}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"Error procesando mensaje: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


connection, channel = connect_rabbitmq()

try:
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=on_message_callback
    )
    channel.start_consuming()
except KeyboardInterrupt:
    print("Cerrando el worker de mantenimiento.")
except pika.exceptions.StreamLostError:
    print("Conexión con RabbitMQ perdida. Reiniciando...")
finally:
    if connection and connection.is_open:
        connection.close()