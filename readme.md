# Plataforma de Mantenimiento Predictivo Interactivo

Este proyecto es una simulación completa de un sistema distribuido de IoT Industrial, diseñado para el monitoreo en tiempo real, la detección de fallas y la intervención humana interactiva.

El sistema implementa una **arquitectura híbrida** avanzada que separa el flujo de datos masivos (telemetría) del flujo de decisiones (comandos humanos), utilizando **Kafka** y **RabbitMQ** para sus respectivas fortalezas.

El proyecto completo se levanta con un solo comando (`docker-compose up`) y consta de 10 contenedores: 2 brokers de mensajería y 8 microservicios.



## 🚀 Tecnologías Utilizadas

* **Orquestación:** Docker y Docker Compose
* **Flujo de Datos (Streaming):** Apache Kafka
* **Flujo de Decisiones (Colas):** RabbitMQ (con el plugin `rabbitmq_delayed_message_exchange`)
* **Backend (Microservicios):** Python
* **API y WebSockets:** FastAPI y Uvicorn
* **Frontend:** HTML5, CSS y JavaScript (vainilla)
* **Servidor Frontend:** Nginx

---

## 🏁 Cómo Ejecutar el Proyecto

Para levantar la plataforma completa, solo necesitas tener Docker y Docker Compose instalados.

1.  **Clonar el Repositorio**
    ```bash
    git clone <URL_DE_TU_REPOSITORIO>
    cd proyecto_plataforma
    ```

2.  **Construir y Levantar los Contenedores**
    Este comando construirá las 8 imágenes de los microservicios y levantará los 10 contenedores.
    ```bash
    docker-compose up --build
    ```

3.  **Acceder al Dashboard**
    Una vez que todos los contenedores estén corriendo, abre tu navegador y ve a:
    **[http://localhost/dashboard.html](http://localhost/dashboard.html)**

4.  **Detener el Sistema**
    Para detener todos los contenedores y eliminar la red, presiona `Ctrl+C` en la terminal y luego ejecuta:
    ```bash
    docker-compose down
    ```

---

## 🏗️ Arquitectura y Flujo de Datos

El sistema se divide en dos flujos principales que corren en paralelo y se conectan a través de un "puente".

### 🌊 Flujo 1: El Flujo de Datos (Kafka)
**Propósito:** Ingesta y análisis de telemetría de alta velocidad. Es el responsable del panel izquierdo (**Estado de Planta**).

1.  **`sensor_producer` (Productor)**
    * Simula 10 sensores industriales.
    * Cada 6 segundos, 💙 **publica** 10 mensajes de vibración (JSON) al topic de Kafka `sensor_data`.
    * Usa el `sensor_id` como clave (key) para garantizar el procesamiento en orden.

2.  **`alert_detector` (Procesador Stateful)**
    * El "cerebro" del sistema. 💚 **Consume** de `sensor_data`.
    * Mantiene un historial en memoria (ventana móvil) para cada sensor.
    * Aplica reglas de falla (ej. Crítica > 90, o 3 Advertencias > 75).
    * Si detecta una falla, 💙 **publica** una nueva alerta en los topics `alerts_critical` o `alerts_warning`.

3.  **`plant_monitor_backend` (Consumidor / Servidor WS)**
    * Un servicio híbrido. 💚 **Consume** de los 3 topics de Kafka (`sensor_data`, `alerts_warning`, `alerts_critical`).
    * Mantiene un estado general de la planta en memoria.
    * 🌐 **Expone** un WebSocket en el puerto `8081` (ruta `/ws`).
    * Cada vez que su estado interno cambia, envía el nuevo estado a todos los dashboards conectados.

### 🚦 Flujo 2: El Flujo de Decisiones (RabbitMQ)
**Propósito:** Gestionar tareas complejas, enrutar a humanos y manejar acciones diferidas. Es el responsable del panel derecho (**Consola de Operador**).

4.  **`alert_router` (El Puente)**
    * El servicio que conecta ambos mundos.
    * 💚 **Consume** las alertas de los topics de Kafka (`alerts_critical`, `alerts_warning`).
    * Enriquece el mensaje: genera el `alert_id` y las `options` (botones).
    * 💙 **Publica** la "solicitud de acción" a un *Fanout Exchange* de RabbitMQ llamado `human_alerts`.

5.  **`operator_console_backend` (Consumidor / Servidor WS)**
    * 💚 **Consume** los mensajes del *Fanout Exchange* `human_alerts`.
    * 🌐 **Expone** un WebSocket en el puerto `8082` (ruta `/ws`).
    * Tan pronto como recibe una alerta, la retransmite a todos los dashboards conectados para que se rendericen los botones.

6.  **`dashboard.html` (El Clic Humano)**
    * El operador ve la alerta y los botones en el panel derecho.
    * Al hacer clic, el JavaScript 💙 **envía** una petición `POST` al endpoint `/decide` del `action_dispatcher`.

7.  **`action_dispatcher` (API / Enrutador)**
    * 🌐 **Expone** la API en el puerto `8080` (ruta `POST /decide`).
    * Recibe la decisión del humano.
    * Actúa como un enrutador inteligente de RabbitMQ. 💙 **Publica** el comando en la cola correcta:
        * `APAGADO_INMEDIATO` → va a la cola `critical_actions_queue`.
        * `PROGRAMAR_MANTENIMIENTO_AHORA` → va a la cola `maintenance_queue`.
        * `RECONOCER_Y_ESPERAR_24H` → va al `delayed_maintenance_exchange` (para ser entregado a `maintenance_queue` 24 horas después).

8.  **`actuator_worker` (Consumidor)**
    * Un *worker* simple y dedicado.
    * 💚 **Consume** tareas *solo* de la `critical_actions_queue`.
    * Simula el apagado de emergencia (imprime en el log).

9.  **`maintenance_worker` (Consumidor)**
    * Un *worker* simple y dedicado.
    * 💚 **Consume** tareas de la `maintenance_queue`.
    * Recibe tanto las tareas inmediatas como las retrasadas de 24 horas.
    * Simula la creación de una orden de trabajo (imprime en el log).

---

## 🔩 Estructura de Microservicios (10 Contenedores)

| Servicio | Puerto (Host) | Imagen | Propósito |
| :--- | :--- | :--- | :--- |
| **Kafka** | `19092:19092` | `confluentinc/cp-kafka` | Broker de streaming para el flujo de datos. |
| **RabbitMQ** | `15672:15672` | `rabbitmq:management` | Broker de mensajería para el flujo de decisiones. |
| **dashboard** | `80:80` | `nginx:alpine` | Sirve el `dashboard.html` estático. |
| **sensor_producer** | - | `(custom)` | (Python) 💙 Simula 10 sensores y publica en Kafka. |
| **alert_detector** | - | `(custom)` | (Python) 🧠 Consume de Kafka, aplica reglas, publica alertas en Kafka. |
| **plant_monitor_backend** | `8081:8081` | `(custom)` | (FastAPI) 💚 Consume de Kafka, 🌐 sirve estado por WS 8081. |
| **alert_router** | - | `(custom)` | (Python) 🧠 Puente: Consume de Kafka, 💙 publica en RabbitMQ. |
| **operator_console_backend** | `8082:8082` | `(custom)` | (FastAPI) 💚 Consume de RabbitMQ, 🌐 sirve acciones por WS 8082. |
| **action_dispatcher** | `8080:8080` | `(custom)` | (FastAPI) 🌐 Recibe `POST /decide`, 💙 enruta acciones a RabbitMQ. |
| **actuator_worker** | - | `(custom)` | (Python) 💚 Consume de `critical_actions_queue`. |
| **maintenance_worker** | - | `(custom)` | (Python) 💚 Consume de `maintenance_queue`. |

---

## 🔧 Configuración Clave

### Tiempos
* **Generación de Sensores:** El `sensor_producer` está configurado para enviar datos cada **6 segundos** (`producer.py`).
* **Retraso de 24H:** El `action_dispatcher` usa el plugin `rabbitmq_delayed_message_exchange` para encolar tareas con retraso.

### CORS
* El `action_dispatcher` está configurado con `CORSMiddleware` (`dispatcher.py`) para permitir que el `dashboard.html` (servido desde el puerto 80) pueda enviar la petición `POST` al puerto 8080.