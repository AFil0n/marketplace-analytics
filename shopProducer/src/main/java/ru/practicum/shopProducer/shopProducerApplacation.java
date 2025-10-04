package ru.practicum.shopProducer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.practicum.common.config.KafkaProperties;
import ru.practicum.common.utils.JsonFileManager;
import ru.practicum.common.model.Product;
import ru.practicum.common.utils.SchemaRegistryHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

@Slf4j
public class shopProducerApplacation {
    private static final String dir = "/etc/data";
    private static final String readyDir = "/etc/ready";

    private static final String SCHEMA_PATH = "/etc/schema/product.json";

    public static void main(String[] args) {
        Properties PROPERTIES = KafkaProperties.getProducerProperties();

        try (Producer<String, Product> producer = new KafkaProducer<>(PROPERTIES)) {
            registerSchema();
            publishingProducts(producer);
        }
    }

    private static void registerSchema(){
        String schemaString;
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(KafkaProperties.getSchemaRegistryUrl(), 10, KafkaProperties.getSchemaRegistryClientProps());

        try{
            schemaString = loadSchemaFromFile();
            SchemaRegistryHelper.registerSchema(schemaRegistryClient, KafkaProperties.getShopProducerTopicName() + "-value", schemaString);
        } catch (Exception e) {
            e.printStackTrace();
        }

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

                JsonFileManager.moveFile(path, readyDir);
            } catch (Exception e) {
                e.printStackTrace();
            }

            for (Product product : products) {
                try {
                    producer.send(new ProducerRecord<>(KafkaProperties.getShopProducerTopicName(), product)).get();
                    log.info("Публикуем в {}: {}", KafkaProperties.getShopProducerTopicName(), product);
                } catch (Exception e) {
                    log.error("Ошибка при публикации сообщения", e);
                }
            }
        }
    }
}
