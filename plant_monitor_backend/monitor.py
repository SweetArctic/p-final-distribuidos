import asyncio
import json
import threading
import time
from datetime import datetime, timezone
from typing import List, Dict, Any

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from kafka import KafkaConsumer

KAFKA_BROKER = 'kafka:9092'
TOPICS = ['sensor_data', 'alerts_warning', 'alerts_critical']

print("Iniciando plant_monitor_backend...")

app = FastAPI()

# --- Estado Global ---
plant_state: Dict[str, Any] = {
    "plant_id": "P-100",
    "status": "OPERATIONAL",
    "monitored_sensors": 0,
    "active_alerts": 0,
    "last_update": None,
    "sensors": {}
}
state_lock = threading.Lock()

# --- Gestor de Conexiones WebSocket ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast_state(self):
        with state_lock:
            # Calcular sumarios antes de enviar
            sensors = plant_state["sensors"]
            plant_state["monitored_sensors"] = len(sensors)
            
            alert_count = sum(1 for s in sensors.values() if s["status"] != "OPERATIONAL")
            plant_state["active_alerts"] = alert_count
            
            if alert_count > 0:
                plant_state["status"] = "CRITICAL" if any(s["status"] == "CRITICAL" for s in sensors.values()) else "WARNING"
            else:
                plant_state["status"] = "OPERATIONAL"
                
            plant_state["last_update"] = datetime.now(timezone.utc).isoformat()
            
            message_to_send = {k: v for k, v in plant_state.items() if k != 'sensors'}

        for connection in self.active_connections:
            await connection.send_json(message_to_send)

manager = ConnectionManager()
loop = asyncio.get_event_loop()

# --- Funciones auxiliares para Kafka ---
def _connect_to_kafka():
    """Intenta conectar al broker de Kafka con reintentos."""
    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='latest',
                group_id='plant_monitor_group',
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            print("Monitor: Conectado a Kafka.")
        except Exception as e:
            print(f"Monitor: Error conectando a Kafka: {e}. Reintentando en 5s...")
            time.sleep(5)
    return consumer

def _initialize_sensor(sensor_id: str):
    """Inicializa un sensor en el estado si no existe."""
    if sensor_id not in plant_state["sensors"]:
        plant_state["sensors"][sensor_id] = {"status": "OPERATIONAL"}

def _update_sensor_status(sensor_id: str, topic: str, current_status: str):
    """Actualiza el estado del sensor basado en el topic de Kafka."""
    if topic == 'alerts_critical':
        plant_state["sensors"][sensor_id]["status"] = "CRITICAL"
        print(f"Monitor: Sensor {sensor_id} actualizado a CRITICAL")
    elif topic == 'alerts_warning' and current_status != "CRITICAL":
        plant_state["sensors"][sensor_id]["status"] = "WARNING"
        print(f"Monitor: Sensor {sensor_id} actualizado a WARNING")
    elif topic == 'sensor_data' and current_status not in ["CRITICAL", "WARNING"]:
        plant_state["sensors"][sensor_id]["status"] = "OPERATIONAL"

def _process_kafka_message(message):
    """Procesa un mensaje de Kafka."""
    data = message.value
    sensor_id = data.get('sensor_id')
    if not sensor_id:
        return

    with state_lock:
        _initialize_sensor(sensor_id)
        current_status = plant_state["sensors"][sensor_id]["status"]
        _update_sensor_status(sensor_id, message.topic, current_status)

    asyncio.run_coroutine_threadsafe(manager.broadcast_state(), loop)

# --- Consumidor de Kafka (en un hilo) ---
def kafka_consumer_thread():
    consumer = _connect_to_kafka()
    print("Monitor: Escuchando topics de Kafka...")
    
    for message in consumer:
        try:
            _process_kafka_message(message)
        except json.JSONDecodeError:
            print(f"Monitor: Error al decodificar mensaje: {message.value}")
        except Exception as e:
            print(f"Monitor: Error procesando mensaje: {e}")

# --- Endpoints de la App ---
@app.on_event("startup")
def startup_event():
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()
    print("Servicio de monitor iniciado y hilo de Kafka corriendo.")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    print("Nuevo cliente de dashboard conectado.")
    # Enviar estado actual al conectarse
    await manager.broadcast_state() 
    try:
        while True:
            # Mantener la conexión abierta
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("Cliente de dashboard desconectado.")

# --- Punto de Entrada ---
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)