package ru.practicum.shopProducer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
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
import ru.practicum.common.utils.SchemaRegistryHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class shopProducerApplacation {
    private static final Properties PROPERTIES;
    private static final String dir = "/etc/data";
    private static final String TOPIC_NAME = "shopTopic";
    private static final String schemaRegistryUrl = "http://schema-registry:8081";
    private static final String USER = "testUser";
    private static final String PASS = "password";
    private static final String SCHEMA_PATH = "/etc/schema/product.json";
    private static final String SR_TS_FILE = "/etc/kafka/secrets/kafka-0.crt";
    private static final String SR_TS_PASS = "";
    private static final String SUBJECT_NAME = TOPIC_NAME + "-value";

    static {
        PROPERTIES = new Properties();

        // Основные настройки Kafka
        PROPERTIES.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:1090,kafka-1:2090");
        PROPERTIES.put(ProducerConfig.ACKS_CONFIG, "all");
        PROPERTIES.put(ProducerConfig.RETRIES_CONFIG, 3);

        // СЕРИАЛИЗАТОРЫ - только один способ!
        PROPERTIES.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        PROPERTIES.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
        // Настройки Schema Registry
        PROPERTIES.put("schema.registry.url", schemaRegistryUrl);
        PROPERTIES.put("basic.auth.credentials.source", "URL");
        PROPERTIES.put("auto.register.schemas", "true");
        PROPERTIES.put("use.latest.version", "true");

        PROPERTIES.put("json.fail.invalid.schema", "false");
        PROPERTIES.put("json.use.optional.for.non.required", "true");
        PROPERTIES.put("oneof.for.nullables", "false");


        // SSL настройки для Schema Registry
        PROPERTIES.put("schema.registry.ssl.truststore.location", "");
        PROPERTIES.put("schema.registry.ssl.truststore.type", "");
        PROPERTIES.put("schema.registry.ssl.truststore.password", "");

        // Настройки безопасности Kafka
        PROPERTIES.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        PROPERTIES.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        PROPERTIES.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"producer\" " +
                        "password=\"password\";");

        // SSL Config для Kafka
        PROPERTIES.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
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
        PROPERTIES.put("schema.registry.log.service.errors", "true");
    }

    public static void main(String[] args) {
        try (Producer<String, Product> producer = new KafkaProducer<>(PROPERTIES)) {
            registerSchema();
            publishingProducts(producer);
        }
    }

    private static void registerSchema(){
        String schemaString;
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10, getSchemaRegistryClientProps());

        try{
            schemaString = loadSchemaFromFile();
            SchemaRegistryHelper.registerSchema(schemaRegistryClient, SUBJECT_NAME, schemaString);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static Map<String, Object> getSchemaRegistryClientProps(){
        Map<String, Object> props = new HashMap<>();
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("basic.auth.credentials.source", "URL");
        //props.put("basic.auth.credentials.source", "USER_INFO");
        //props.put("basic.auth.user.info", USER + ":" + PASS);
        props.put("schema.registry.ssl.truststore.location", "");
        //props.put("schema.registry.ssl.truststore.type", "PEM");
        props.put("schema.registry.ssl.truststore.password", "");

        // Добавьте дополнительные параметры
        //props.put("schema.registry.ssl.protocol", "TLSv1.2");
        //props.put("schema.registry.ssl.enabled.protocols", "TLSv1.2");
        props.put("schema.registry.ssl.endpoint.identification.algorithm", "");

        return props;
    }

    private static String loadSchemaFromFile() throws IOException {
        Path path = Path.of(SCHEMA_PATH);
        return Files.readString(path);
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
