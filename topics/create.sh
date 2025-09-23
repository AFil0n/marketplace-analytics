#!/bin/bash

# Используем kafka-0 как брокер, так как он доступен внутри сети Docker
KAFKA_BROKER="kafka-0:1092,kafka-1:2092"
CLIENT_CONFIG="/etc/kafka/secrets/client.properties"

# Ждем готовности Kafka
echo "Waiting for Kafka brokers to be ready..."
for broker in kafka-0:1092 kafka-1:2092; do
  while ! nc -z $(echo $broker | tr ':' ' '); do
    echo "Waiting for $broker..."
    sleep 3
  done
done

sleep 15

# Создаем топики
echo "Creating topics..."
kafka-topics --bootstrap-server $KAFKA_BROKER --create \
  --topic topic-1 --partitions 2 --replication-factor 2 \
  --command-config $CLIENT_CONFIG

kafka-topics --bootstrap-server $KAFKA_BROKER --create \
  --topic topic-2 --partitions 2 --replication-factor 2 \
  --command-config $CLIENT_CONFIG

# Настройка ACL для пользователей
echo "Setting up ACLs..."

# Для topic-1
kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:producer \
  --operation WRITE --topic topic-1 \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation READ \
  --topic topic-1 \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation DESCRIBE \
  --topic topic-1 \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation READ \
  --group consumer-group \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation DESCRIBE \
  --group consumer-group \
  --command-config $CLIENT_CONFIG

# Для topic-2 (пример с ограничением доступа)
kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:producer \
  --operation WRITE --topic topic-2 \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --deny-principal User:consumer \
  --operation READ --topic topic-2 \
  --command-config $CLIENT_CONFIG


echo "Топики и ACL успешно настроены"

sleep 30

kafka-acls --bootstrap-server $KAFKA_BROKER --command-config $CLIENT_CONFIG --list


