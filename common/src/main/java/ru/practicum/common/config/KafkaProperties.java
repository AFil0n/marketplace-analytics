package ru.practicum.common.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaProperties {

    private static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";
    private static final String SHOP_PRODUCER_TOPIC_NAME = "shopTopic";
    private static final String TOPIC_BLOCKED_PRODUCTS = "blocked-products";
    private static final String PRODUCTS_TOPIC_NAME = "products";

    public static String getShopProducerTopicName() {
        return SHOP_PRODUCER_TOPIC_NAME;
    }

    public static String getSchemaRegistryUrl() {
        return SCHEMA_REGISTRY_URL;
    }

    public static String getTopicBlockedProducts() {
        return TOPIC_BLOCKED_PRODUCTS;
    }

    public static String getProductsTopicName() {
        return PRODUCTS_TOPIC_NAME;
    }

    public static Properties getStreamsConfig(){
        Properties props = new Properties();

        // ОБЯЗАТЕЛЬНЫЕ НАСТРОЙКИ
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-filter-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:1092,kafka-1:2092");

        // СЕРИАЛИЗАТОРЫ
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // НАСТРОЙКИ ОБРАБОТКИ ОШИБОК
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.DefaultProductionExceptionHandler");

        // НАСТРОЙКИ ПОТРЕБИТЕЛЯ
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // НАСТРОЙКИ ГРУППОВОГО ПРОТОКОЛА
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 15000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 40000);

        // НАСТРОЙКИ БЕЗОПАСНОСТИ
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"admin\" " +
                        "password=\"admin\";");

        // SSL НАСТРОЙКИ
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/kafka/secrets/kafka.truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "password");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/etc/kafka/secrets/kafka.keystore.pkcs12");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "password");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "password");

        return props;
    }

    public static Properties getProducerProperties() {
        Properties props = new Properties();

        // Основные настройки Kafka
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:1090,kafka-1:2090");

        // СЕРИАЛИЗАТОРЫ - только один способ!
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");

        // Настройки Schema Registry
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put("basic.auth.credentials.source", "URL");
        props.put("auto.register.schemas", "true");
        props.put("use.latest.version", "true");

        props.put("json.fail.invalid.schema", "false");
        props.put("json.use.optional.for.non.required", "true");
        props.put("oneof.for.nullables", "false");


        // SSL настройки для Schema Registry
        props.put("schema.registry.ssl.truststore.location", "");
        props.put("schema.registry.ssl.truststore.type", "");
        props.put("schema.registry.ssl.truststore.password", "");

        // Настройки безопасности Kafka
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"producer\" " +
                        "password=\"password\";");

        // SSL Config для Kafka
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                "/etc/kafka/secrets/kafka.truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "password");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                "/etc/kafka/secrets/kafka.keystore.pkcs12");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "password");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "password");

        // Дополнительные настройки
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "producer-app");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put("schema.registry.log.service.errors", "true");

        return props;
    }

    public static Properties getConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public static Map<String, Object> getSchemaRegistryClientProps(){
        Map<String, Object> props = new HashMap<>();
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put("basic.auth.credentials.source", "URL");
        props.put("schema.registry.ssl.truststore.location", "");
        props.put("schema.registry.ssl.truststore.password", "");
        props.put("schema.registry.ssl.endpoint.identification.algorithm", "");

        return props;
    }
}
