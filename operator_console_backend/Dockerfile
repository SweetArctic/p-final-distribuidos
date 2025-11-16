# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar el archivo de requisitos e instalar las tendencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el script de la consola
COPY console.py .

# Exponer el puerto del WebSocket
EXPOSE 8082

# Comando para ejecutar el servidor
CMD ["uvicorn", "console:app", "--host", "0.0.0.0", "--port", "8082"]