import json
import os
import pika
import uvicorn
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from contextlib import contextmanager

RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'user')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'password')
CRITICAL_QUEUE = 'critical_actions_queue'
MAINTENANCE_QUEUE = 'maintenance_queue'
DELAYED_EXCHANGE = 'delayed_maintenance_exchange'
DELAY_24H_MS = 24 * 60 * 60 * 1000 # 24 horas en milisegundos

print("Iniciando action_dispatcher...")

app = FastAPI()

# --- Modelo de Pydantic para la solicitud POST ---
class Decision(BaseModel):
    alert_id: str
def get_rabbitmq_connection():
    try:
        # Evitar credenciales embebidas: usar las variables de entorno
        if RABBITMQ_USER == 'user' or RABBITMQ_PASSWORD == 'password':
            print("Dispatcher: WARNING: Using default RabbitMQ credentials; change them immediately.")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=5672, credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD))
        )
        print("Dispatcher: Conectado a RabbitMQ.")
        return connection
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Dispatcher: Error conectando a RabbitMQ: {e}")
        raise

@contextmanager
def get_channel():
    connection = None
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        # Declarar colas estándar
        channel.queue_declare(queue=CRITICAL_QUEUE, durable=True)
        channel.queue_declare(queue=MAINTENANCE_QUEUE, durable=True)
        
        # Declarar el Exchange con retraso
        # Se necesita el plugin 'rabbitmq_delayed_message_exchange'
        channel.exchange_declare(
            exchange=DELAYED_EXCHANGE,
            exchange_type='x-delayed-message',
            durable=True,
            arguments={'x-delayed-type': 'direct'}
        )
        
        # Vincular la cola de mantenimiento al exchange con retraso
        channel.queue_bind(
            queue=MAINTENANCE_QUEUE,
            exchange=DELAYED_EXCHANGE,
            routing_key='maintenance.delayed'
        )
        
        yield channel
    except pika.exceptions.AMQPConnectionError:
        raise HTTPException(status_code=503, detail="Error de conexión con RabbitMQ")
    finally:
        if connection and connection.is_open:
            connection.close()

# --- Endpoint de Decisión ---
@app.post("/decide")
async def decide_action(decision: Decision, background_tasks: BackgroundTasks):
    print(f"Dispatcher: Decisión recibida para {decision.alert_id}: {decision.chosen_action}")
    
    # Usamos tareas en segundo plano para no bloquear la respuesta HTTP
    background_tasks.add_task(route_decision, decision)
    
    return {"status": "decision_received", "alert_id": decision.alert_id, "action": decision.chosen_action}

def route_decision(decision: Decision):
    try:
        with get_channel() as channel:
            action_body = json.dumps({"alert_id": decision.alert_id, "action": decision.chosen_action})
            
            if decision.chosen_action == "APAGADO_INMEDIATO":
                # Publica en la cola de acciones críticas [cite: 30]
                channel.basic_publish(
                    exchange='',
                    routing_key=CRITICAL_QUEUE,
                    body=action_body,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"Enrutado {decision.alert_id} a {CRITICAL_QUEUE}")

            elif decision.chosen_action == "PROGRAMAR_MANTENIMIENTO_AHORA":
                # Publica en la cola de mantenimiento (sin retraso) [cite: 31]
                channel.basic_publish(
                    exchange='',
                    routing_key=MAINTENANCE_QUEUE,
                    body=action_body,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"Enrutado {decision.alert_id} a {MAINTENANCE_QUEUE} (ahora)")

            elif decision.chosen_action == "RECONOCER_Y_ESPERAR_24H":
                # Publica en el Exchange con retraso 
                properties = pika.BasicProperties(
                    delivery_mode=2,
                    headers={'x-delay': DELAY_24H_MS}
                )
                channel.basic_publish(
                    exchange=DELAYED_EXCHANGE,
                    routing_key='maintenance.delayed',
                    body=action_body,
                    properties=properties
                )
                print(f"Enrutado {decision.alert_id} a {MAINTENANCE_QUEUE} (con 24h de retraso)")

            elif decision.chosen_action == "IGNORAR_10_MINUTOS":
                # Esta acción no tiene un worker, solo se registra
                print(f"Acción {decision.chosen_action} para {decision.alert_id} registrada. No se enruta a workers.")
            
            else:
                print(f"Acción desconocida: {decision.chosen_action}")

    except Exception as e:
        print(f"Error al enrutar la decisión {decision.alert_id}: {e}")

# --- Punto de Entrada ---
if __name__ == "__main__":
    # Asegurarse de que las colas existan al inicio
    try:
        with get_channel():
            print("Dispatcher: Colas y exchanges verificados al inicio.")
    except Exception as e:
        print(f"Error crítico al iniciar: No se pudo conectar a RabbitMQ. {e}")
        # En un escenario real, esto debería reintentarse o fallar
    
    uvicorn.run(app, host="0.0.0.0", port=8080)