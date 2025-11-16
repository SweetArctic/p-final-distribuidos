# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar el archivo de requisitos e instalar las dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el script del dispatcher
COPY dispatcher.py .

# Exponer el puerto de la API
EXPOSE 8080

# Comando para ejecutar el servidor
CMD ["uvicorn", "dispatcher:app", "--host", "0.0.0.0", "--port", "8080"]