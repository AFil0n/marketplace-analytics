FROM confluentinc/cp-kafka-connect:7.7.1

# Скопировать JMX Exporter в докер образ
COPY jmx_prometheus_javaagent-0.15.0.jar /opt/
# Скопировать настройки JMX Exporter в докер образ
COPY kafka-connect.yml /opt/

# Установка необходимых утилит
RUN apt-get update && apt-get install -y curl jq && rm -rf /var/lib/apt/lists/*

# Копирование конфигурации и плагинов
COPY confluent-hub-components/ /etc/kafka-connect/jars/
COPY pg-config.json /etc/kafka-connect/
COPY init-connector.sh /usr/local/bin/

# Установка прав
RUN chmod +x /usr/local/bin/init-connector.sh

# Точка входа с автоматической инициализацией
CMD ["bash", "-c", "\
  /etc/confluent/docker/run & \
  while ! curl -s http://localhost:8083/ >/dev/null; do sleep 5; done; \
  /usr/local/bin/init-connector.sh; \
  tail -f /dev/null\
"]