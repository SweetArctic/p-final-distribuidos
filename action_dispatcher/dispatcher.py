import json
import pika
import uvicorn
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from contextlib import contextmanager
from fastapi.middleware.cors import CORSMiddleware

RABBITMQ_HOST = 'rabbitmq'
CRITICAL_QUEUE = 'critical_actions_queue'
MAINTENANCE_QUEUE = 'maintenance_queue'
ACKNOWLEDGED_CRITICAL_QUEUE = 'acknowledged_critical_queue' # Nueva cola
DELAYED_EXCHANGE = 'delayed_maintenance_exchange'
DELAY_24H_MS = 24 * 60 * 60 * 1000

print("Iniciando action_dispatcher...")

app = FastAPI()
origins = ["*"]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class Decision(BaseModel):
    chosen_action: str
    alert: dict # Recibimos la alerta completa

def get_rabbitmq_connection():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=5672, credentials=pika.PlainCredentials('user', 'password')))
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
        
        channel.queue_declare(queue=CRITICAL_QUEUE, durable=True)
        channel.queue_declare(queue=MAINTENANCE_QUEUE, durable=True)
        channel.queue_declare(queue=ACKNOWLEDGED_CRITICAL_QUEUE, durable=True) # Declarar nueva cola

        channel.exchange_declare(
            exchange=DELAYED_EXCHANGE,
            exchange_type='x-delayed-message',
            durable=True,
            arguments={'x-delayed-type': 'direct'}
        )
        channel.queue_bind(queue=MAINTENANCE_QUEUE, exchange=DELAYED_EXCHANGE, routing_key='maintenance.delayed')
        
        yield channel
    except pika.exceptions.AMQPConnectionError:
        raise HTTPException(status_code=503, detail="Error de conexión con RabbitMQ")
    finally:
        if connection and connection.is_open:
            connection.close()

@app.post("/decide")
async def decide_action(decision: Decision, background_tasks: BackgroundTasks):
    alert_id = decision.alert.get("alert_id", "N/A")
    print(f"Dispatcher: Decisión recibida para {alert_id}: {decision.chosen_action}")
    background_tasks.add_task(route_decision, decision)
    return {"status": "decision_received", "alert_id": alert_id, "action": decision.chosen_action}

def route_decision(decision: Decision):
    try:
        with get_channel() as channel:
            alert = decision.alert
            alert_id = alert.get("alert_id", "N/A")
            
            if decision.chosen_action == "APAGADO_INMEDIATO":
                sensor_id = alert.get("sensor_id")
                if not sensor_id:
                    print(f"Error: No se encontró sensor_id en la alerta {alert_id}")
                    return
                
                action_body = json.dumps({"action": decision.chosen_action, "sensor_id": sensor_id})
                channel.basic_publish(
                    exchange='',
                    routing_key=CRITICAL_QUEUE,
                    body=action_body,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"Enrutado apagado del sensor {sensor_id} a {CRITICAL_QUEUE}")

            elif decision.chosen_action == "IGNORAR_10_MINUTOS":
                # Publicar la alerta completa en la nueva cola
                channel.basic_publish(
                    exchange='',
                    routing_key=ACKNOWLEDGED_CRITICAL_QUEUE,
                    body=json.dumps(alert),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"Enrutada alerta crítica {alert_id} a {ACKNOWLEDGED_CRITICAL_QUEUE} para revisión")

            elif decision.chosen_action == "PROGRAMAR_MANTENIMIENTO_AHORA":
                action_body = json.dumps({"alert_id": alert_id, "action": decision.chosen_action})
                channel.basic_publish(
                    exchange='',
                    routing_key=MAINTENANCE_QUEUE,
                    body=action_body,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"Enrutado {alert_id} a {MAINTENANCE_QUEUE} (ahora)")

            elif decision.chosen_action == "RECONOCER_Y_ESPERAR_24H":
                action_body = json.dumps({"alert_id": alert_id, "action": decision.chosen_action})
                properties = pika.BasicProperties(delivery_mode=2, headers={'x-delay': DELAY_24H_MS})
                channel.basic_publish(
                    exchange=DELAYED_EXCHANGE,
                    routing_key='maintenance.delayed',
                    body=action_body,
                    properties=properties
                )
                print(f"Enrutado {alert_id} a {MAINTENANCE_QUEUE} (con 24h de retraso)")
            
            else:
                print(f"Acción desconocida: {decision.chosen_action}")

    except Exception as e:
        print(f"Error al enrutar la decisión para la alerta {decision.alert.get('alert_id', 'N/A')}: {e}")

if __name__ == "__main__":
    try:
        with get_channel():
             print("Dispatcher: Colas y exchanges verificados al inicio.")
    except Exception as e:
        print(f"Error crítico al iniciar: No se pudo conectar a RabbitMQ. {e}")
    
    uvicorn.run(app, host="0.0.0.0", port=8080)