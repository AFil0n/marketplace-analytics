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
  --topic shopTopic --partitions 2 --replication-factor 2 \
  --command-config $CLIENT_CONFIG

kafka-topics --bootstrap-server $KAFKA_BROKER --create \
  --topic clientTopic --partitions 2 --replication-factor 2 \
  --command-config $CLIENT_CONFIG

kafka-topics --bootstrap-server $KAFKA_BROKER --create \
  --topic shopStopList --partitions 2 --replication-factor 2 \
  --command-config $CLIENT_CONFIG

kafka-topics --bootstrap-server $KAFKA_BROKER --create \
  --topic products --partitions 2 --replication-factor 2 \
  --command-config $CLIENT_CONFIG

# Настройка ACL для пользователей
echo "Setting up ACLs..."

# Для shopTopic
kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:producer \
  --operation WRITE --topic shopTopic \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation READ \
  --topic shopTopic \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation DESCRIBE \
  --topic shopTopic \
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

# Для clientTopic
kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:producerClient \
  --operation WRITE --topic clientTopic \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation READ \
  --topic clientTopic \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation DESCRIBE \
  --topic clientTopic \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation READ \
  --group consumerClient-group \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation DESCRIBE \
  --group consumerClient-group \
  --command-config $CLIENT_CONFIG


# Для shopStopList
kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:producerClient \
  --operation WRITE --topic shopStopList \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation READ \
  --topic shopStopList \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation DESCRIBE \
  --topic shopStopList \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation READ \
  --group shopStopList-group \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation DESCRIBE \
  --group shopStopList-group \
  --command-config $CLIENT_CONFIG


# Для products
kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:producerClient \
  --operation WRITE --topic products \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation READ \
  --topic products \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation DESCRIBE \
  --topic products \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation READ \
  --group products-group \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation DESCRIBE \
  --group products-group \
  --command-config $CLIENT_CONFIG


echo "Топики и ACL успешно настроены"

sleep 30

kafka-acls --bootstrap-server $KAFKA_BROKER --command-config $CLIENT_CONFIG --list


