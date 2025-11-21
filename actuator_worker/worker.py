import sys
import json
import time
import pika

RABBITMQ_HOST = 'rabbitmq'
INBOUND_QUEUE = 'critical_actions_queue'
SHUTDOWN_QUEUE = 'shutdown_sensor_queue' # Cola de salida hacia el productor

print("Iniciando actuator_worker...")

def connect_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, port=5672, credentials=pika.PlainCredentials('user', 'password'))
            )
            channel = connection.channel()
            
            # Asegurarse de que ambas colas existan
            channel.queue_declare(queue=INBOUND_QUEUE, durable=True)
            channel.queue_declare(queue=SHUTDOWN_QUEUE, durable=True)
            
            channel.basic_qos(prefetch_count=1)
            
            print(f"Actuator Worker: Conectado. Esperando acciones en '{INBOUND_QUEUE}'...")
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Actuator Worker: Error conectando a RabbitMQ: {e}. Reintentando en 5s...")
            time.sleep(5)

def on_message_callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode('utf-8'))
        sensor_id = data.get('sensor_id')
        action = data.get('action')
        
        if not sensor_id:
            print(f"Error: No se encontró 'sensor_id' en el mensaje: {data}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        print("\n--- ¡ACCIÓN CRÍTICA RECIBIDA! ---")
        print(f"  Sensor ID: {sensor_id}")
        print(f"  Acción: {action}")
        
        # Enviar comando de apagado al sensor_producer
        print(f"  ... Reenviando comando de apagado al sensor {sensor_id} ...")
        ch.basic_publish(
            exchange='',
            routing_key=SHUTDOWN_QUEUE,
            body=sensor_id,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"  ... Comando enviado a la cola '{SHUTDOWN_QUEUE}'.")
        
        # Confirmar que el mensaje original fue procesado
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"--- Tarea para sensor {sensor_id} completada. Esperando nuevas acciones. ---")
        
    except json.JSONDecodeError:
        print(f"Error: No se pudo decodificar el mensaje: {body}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"Error procesando mensaje: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


connection, channel = connect_rabbitmq()

try:
    channel.basic_consume(
        queue=INBOUND_QUEUE,
        on_message_callback=on_message_callback
    )
    channel.start_consuming()
except KeyboardInterrupt:
    print("Cerrando el worker de actuador.")
except pika.exceptions.StreamLostError:
    print("Conexión con RabbitMQ perdida. Reiniciando...")
finally:
    if connection and connection.is_open:
        connection.close()