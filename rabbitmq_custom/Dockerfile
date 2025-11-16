FROM rabbitmq:3.13-management

USER root

# Instalar curl
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Descargar el plugin
RUN curl -L https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v3.13.0/rabbitmq_delayed_message_exchange-3.13.0.ez \
    -o /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-3.13.0.ez

# Cambiar permisos
RUN chown rabbitmq:rabbitmq /opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-3.13.0.ez

# Cambiar a usuario rabbitmq
USER rabbitmq

# CRÍTICO: Habilitar el plugin
RUN rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange

# Exponer puertos
EXPOSE 5672 15672