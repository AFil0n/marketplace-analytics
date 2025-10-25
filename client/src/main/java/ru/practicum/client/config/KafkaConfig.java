package ru.practicum.client.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();

        // Основные настройки Kafka
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:1090,kafka-1:2090");

        // СЕРИАЛИЗАТОРЫ - исправлено
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Настройки для JsonSerializer
        configProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false); // Убираем заголовки типа

        // Настройки безопасности Kafka
        configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        configProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        configProps.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"producer\" " +
                        "password=\"password\";");

        // SSL Config для Kafka
        configProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        configProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        configProps.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        configProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                "/etc/kafka/secrets/kafka.truststore.jks");
        configProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "password");
        configProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                "/etc/kafka/secrets/kafka.keystore.pkcs12");
        configProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "password");
        configProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "password");

        // Дополнительные настройки
        configProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, "producer-app");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}