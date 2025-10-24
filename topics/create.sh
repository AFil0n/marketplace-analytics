#!/bin/bash

# Используем kafka-0 как брокер, так как он доступен внутри сети Docker
KAFKA_BROKER="kafka-0:1090,kafka-1:2090"
CLIENT_CONFIG="/etc/kafka/secrets/client.properties"

# Ждем готовности Kafka
echo "Waiting for Kafka brokers to be ready..."
for broker in kafka-0:1090 kafka-1:2090; do
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
  --topic products --partitions 2 --replication-factor 2 \
  --command-config $CLIENT_CONFIG

kafka-topics --bootstrap-server $KAFKA_BROKER --create \
  --topic blocked-products --partitions 2 --replication-factor 2 \
  --command-config $CLIENT_CONFIG

  kafka-topics --bootstrap-server $KAFKA_BROKER --create \
    --topic userQuery --partitions 2 --replication-factor 2 \
    --command-config $CLIENT_CONFIG

 kafka-topics --bootstrap-server $KAFKA_BROKER --create \
    --topic user_recommendations --partitions 2 --replication-factor 2 \
    --command-config $CLIENT_CONFIG


    for topic in "${SYSTEM_TOPICS[@]}"; do
        echo "Creating topic: $topic"
        kafka-topics --bootstrap-server kafka-dr-0:3090 \
            --command-config /etc/kafka/secrets/client.properties \
            --create --topic "$topic" \
            --partitions 2 --replication-factor 2 \
            --config cleanup.policy=compact 2>/dev/null && echo "✅ Created $topic" || echo "⚠️ Topic $topic may already exist"
    done

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
  --add --allow-principal User:consumer \
  --operation READ \
  --topic clientTopic \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation DESCRIBE \
  --topic clientTopic \
  --command-config $CLIENT_CONFIG


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

# Для products

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation READ \
  --topic products \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation DESCRIBE \
  --topic products \
  --command-config $CLIENT_CONFIG

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


# blocked-products
kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:producer \
  --operation WRITE --topic blocked-products \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation READ --topic blocked-products \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation DESCRIBE --topic blocked-products \
  --command-config $CLIENT_CONFIG


# Для shopStopList
kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:producerClient \
  --operation WRITE --topic shopStopList \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation READ \
  --topic userQuery \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumerClient \
  --operation DESCRIBE \
  --topic userQuery \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation READ \
  --topic userQuery \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation DESCRIBE \
  --topic userQuery \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation READ \
  --group userQuery-group \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:consumer \
  --operation DESCRIBE \
  --group userQuery-group \
  --command-config $CLIENT_CONFIG

kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:producer \
  --operation WRITE --topic userQuery \
  --command-config $CLIENT_CONFIG

#user_recommendations
kafka-acls --bootstrap-server $KAFKA_BROKER \
  --add --allow-principal User:producer \
  --operation WRITE --topic user_recommendations \
  --command-config $CLIENT_CONFIG

echo "Топики и ACL успешно настроены"

sleep 30

kafka-acls --bootstrap-server $KAFKA_BROKER --command-config $CLIENT_CONFIG --list


