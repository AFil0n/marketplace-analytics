#!/bin/bash

# Минимальная задержка
sleep 10

# Простые конфиги
echo 'bootstrap.servers=kafka-0:1090,kafka-1:2090' > /tmp/consumer.properties
echo 'group.id=mirror-group' >> /tmp/consumer.properties
echo 'security.protocol=SASL_SSL' >> /tmp/consumer.properties
echo 'sasl.mechanism=PLAIN' >> /tmp/consumer.properties
echo 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";' >> /tmp/consumer.properties
echo 'ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks' >> /tmp/consumer.properties
echo 'ssl.truststore.password=password' >> /tmp/consumer.properties
echo 'ssl.endpoint.identification.algorithm=' >> /tmp/consumer.properties

echo 'bootstrap.servers=kafka-dr-0:3090,kafka-dr-1:3190' > /tmp/producer.properties
echo 'security.protocol=SASL_SSL' >> /tmp/producer.properties
echo 'sasl.mechanism=PLAIN' >> /tmp/producer.properties
echo 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";' >> /tmp/producer.properties
echo 'ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks' >> /tmp/producer.properties
echo 'ssl.truststore.password=password' >> /tmp/producer.properties
echo 'ssl.endpoint.identification.algorithm=' >> /tmp/producer.properties

# Запуск одной командой
kafka-mirror-maker --consumer.config /tmp/consumer.properties --producer.config /tmp/producer.properties --whitelist '.*' --num.streams 1