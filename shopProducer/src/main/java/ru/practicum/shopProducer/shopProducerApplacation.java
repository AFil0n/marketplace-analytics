package ru.practicum.shopProducer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.practicum.common.utils.JsonFileManager;
import ru.practicum.common.model.Product;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class shopProducerApplacation {
    private static final Properties PROPERTIES;
    private static final String dir = "data";
    private static final String TOPIC_NAME = "shopTopic";
    private static final String schemaRegistryUrl = "https://localhost:8081";
    private static final String USER = "testUser";
    private static final String PASS = "password";

    static {
        PROPERTIES = new Properties();
        // Основные настройки
        PROPERTIES.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:1090,kafka-1:2090");
        PROPERTIES.put(ProducerConfig.ACKS_CONFIG, "all");
        PROPERTIES.put(ProducerConfig.RETRIES_CONFIG, 3);
        PROPERTIES.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        // Настройки безопасности
        PROPERTIES.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        PROPERTIES.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        PROPERTIES.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"producer\" " +
                        "password=\"password\";");

        // SSL Config
        PROPERTIES.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        PROPERTIES.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        PROPERTIES.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                "/etc/kafka/secrets/kafka.truststore.jks");
        PROPERTIES.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "password");
        PROPERTIES.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                "/etc/kafka/secrets/kafka.keystore.pkcs12");
        PROPERTIES.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "password");
        PROPERTIES.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "password");

        // Дополнительные настройки
        PROPERTIES.put(CommonClientConfigs.CLIENT_ID_CONFIG, "producer-app");
        PROPERTIES.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        PROPERTIES.put("javax.net.debug", "ssl");

        PROPERTIES.put("schema.registry.url", schemaRegistryUrl);
        PROPERTIES.put("schema.registry.basic.auth.user.info", USER + ":" + PASS);

        PROPERTIES.put("schema.registry.ssl.truststore.location", "user");
        PROPERTIES.put("schema.registry.ssl.truststore.type", "PEM");
        PROPERTIES.put("schema.registry.ssl.truststore.password", "password");
    }

    public static void main(String[] args) {
        try (Producer<String, Product> producer = new KafkaProducer<>(PROPERTIES)) {
            publishingProducts(producer);
        }
    }

    private static List<Product> getFileProducts(String path) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.findAndRegisterModules();

            File file = new File(path);
            return mapper.readValue(file, new TypeReference<List<Product>>() {});
        }catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    private static void publishingProducts(Producer<String, Product> producer) {
        //TODO добавить schemaRegistry
        List<Product> products = null;

        while (true) {
            try{
                Path path = JsonFileManager.getFirstJsonFile(dir);
                products = getFileProducts(path.toString());

                if(products.isEmpty() || products == null){
                    Thread.sleep(1000L);
                    log.info("Не удалось получить  из файла: {}", dir.toString());
                    JsonFileManager.removeFile(path.toString());
                    continue;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            for (Product product : products) {
                try {
                    Thread.sleep(1000L);
                    log.info("Публикуем в {}: {}", TOPIC_NAME, product);
                    producer.send(new ProducerRecord<>(TOPIC_NAME, product));
                } catch (Exception e) {
                    log.error("Ошибка при публикации сообщения", e);
                }
            }
        }
    }
}
