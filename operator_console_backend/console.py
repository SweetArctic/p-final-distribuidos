import asyncio
import json
import threading
import time
import pika

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import List

RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_EXCHANGE = 'human_alerts'

print("Iniciando operator_console_backend...")

app = FastAPI()

# --- Gestor de Conexiones WebSocket ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast_json(self, data: dict):
        for connection in self.active_connections:
            await connection.send_json(data)

manager = ConnectionManager()
loop = asyncio.get_event_loop()

# --- Consumidor de RabbitMQ (en un hilo) ---
def rabbitmq_consumer_thread():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, port=5672, credentials=pika.PlainCredentials('user', 'password'))
            )
            channel = connection.channel()
            
            channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type='fanout')
            
            # Declarar una cola exclusiva (temporal)
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            
            # Vincular la cola al exchange
            channel.queue_bind(exchange=RABBITMQ_EXCHANGE, queue=queue_name)
            
            print(f"Console: Conectado a RabbitMQ. Esperando alertas en '{queue_name}'...")

            def callback(ch, method, properties, body):
                try:
                    message_data = json.loads(body.decode('utf-8'))
                    print(f"Console: Alerta recibida de RabbitMQ: {message_data['alert_id']}")
                    
                    # Programar el broadcast en el loop de eventos de asyncio
                    asyncio.run_coroutine_threadsafe(
                        manager.broadcast_json(message_data), 
                        loop
                    )
                except json.JSONDecodeError:
                    print(f"Console: Error al decodificar JSON de RabbitMQ: {body}")
                except Exception as e:
                    print(f"Console: Error en callback: {e}")
            
            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
            
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"Console: Error de conexión con RabbitMQ: {e}. Reintentando en 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"Console: Error inesperado en el hilo de RabbitMQ: {e}. Reiniciando...")
            time.sleep(5)

# --- Endpoints de la App ---
@app.on_event("startup")
def startup_event():
    threading.Thread(target=rabbitmq_consumer_thread, daemon=True).start()
    print("Servicio de consola iniciado y hilo de RabbitMQ corriendo.")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    print("Nuevo cliente de consola (dashboard) conectado.")
    try:
        while True:
            # Mantener la conexión abierta
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("Cliente de consola (dashboard) desconectado.")

# --- Punto de Entrada ---
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8082)