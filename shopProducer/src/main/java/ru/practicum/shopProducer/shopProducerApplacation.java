package ru.practicum.shopProducer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.practicum.common.config.KafkaProperties;
import ru.practicum.common.dto.ProductDTO;
import ru.practicum.common.utils.JsonFileManager;
import ru.practicum.common.utils.SchemaRegistryHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Slf4j
public class shopProducerApplacation {
    private static final String dir = "/etc/data";
    private static final String readyDir = "/etc/ready";

    private static final String SCHEMA_PATH = "/etc/schema/product.json";

    public static void main(String[] args) {
        Properties PROPERTIES = KafkaProperties.getProducerProperties();

        try (Producer<String, ProductDTO> producer = new KafkaProducer<>(PROPERTIES)) {
            registerSchema();
            publishingProducts(producer);
        }
    }

    private static void registerSchema() {
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(
                KafkaProperties.getSchemaRegistryUrl(), 10, KafkaProperties.getSchemaRegistryClientProps());

        try {

            try {
                schemaRegistryClient.deleteSubject("shopTopic-value");
                System.out.println("✅ Deleted shopTopic-value");
            } catch (Exception e) {
                System.out.println("ℹ️ shopTopic-value already deleted or not exists");
            }

            try {
                schemaRegistryClient.deleteSubject("products-value");
                System.out.println("✅ Deleted products-value");
            } catch (Exception e) {
                System.out.println("ℹ️ products-value already deleted or not exists");
            }

            String schemaString = loadSchemaFromFile();

            // Регистрируем схемы
            SchemaRegistryHelper.registerSchema(schemaRegistryClient, "shopTopic-value", schemaString);
            SchemaRegistryHelper.registerSchema(schemaRegistryClient, "products-value", schemaString);

            // ПРИНУДИТЕЛЬНАЯ ПРОВЕРКА
            System.out.println("🔍 Verifying registration...");
            checkSchemasRegistered(schemaRegistryClient);

        } catch (Exception e) {
            System.err.println("❌ Schema registration failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void checkSchemasRegistered(SchemaRegistryClient client) {
        try {
            List<String> subjects = (List<String>) client.getAllSubjects();
            System.out.println("📋 Available subjects: " + subjects);

            // Проверяем конкретные схемы
            String[] requiredSubjects = {"shopTopic-value", "products-value"};
            for (String subject : requiredSubjects) {
                try {
                    SchemaMetadata metadata = client.getLatestSchemaMetadata(subject);
                    System.out.println("✅ " + subject + " - FOUND (ID: " + metadata.getId() + ")");
                } catch (Exception e) {
                    System.err.println("❌ " + subject + " - NOT FOUND");
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Cannot check registered schemas: " + e.getMessage());
        }
    }

    private static String loadSchemaFromFile() throws IOException {
        Path path = Path.of(SCHEMA_PATH);
        return Files.readString(path);
    }

    private static List<ProductDTO> getFileProducts(String path) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.findAndRegisterModules();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            File file = new File(path);
            if (!file.exists()) {
                log.error("❌ File not found: {}", path);
                return Collections.emptyList();
            }

            List<ProductDTO> products = mapper.readValue(file, new TypeReference<List<ProductDTO>>() {});
            log.debug("📄 Loaded {} products from: {}", products.size(), path);
            return products;

        } catch (Exception e) {
            log.error("❌ Failed to parse products from {}: {}", path, e.getMessage());
            return Collections.emptyList(); // Возвращаем пустой список вместо null
        }
    }

    private static void publishingProducts(Producer<String, ProductDTO> producer) {
        while (true) {
            try {
                Path path = JsonFileManager.getFirstJsonFile(dir);
                if (path == null) {
                    log.info("⏳ No JSON files found, waiting...");
                    Thread.sleep(5000L);
                    continue;
                }

                List<ProductDTO> products = getFileProducts(path.toString());

                if (products == null || products.isEmpty()) {
                    log.warn("⚠️ No products found, removing file...");
                    JsonFileManager.removeFile(path.toString());
                    continue;
                }

                log.info("✅ Found {} products in file: {}", products.size(), path.getFileName());

                int successCount = 0;
                int errorCount = 0;

                for (ProductDTO product : products) {
                    try {
                        Thread.sleep(50L);

                        if (!isValidProduct(product)) {
                            log.warn("⚠️ Invalid product skipped: {}", product.getProductId());
                            errorCount++;
                            continue;
                        }

                        producer.send(new ProducerRecord<>(KafkaProperties.getShopProducerTopicName(), product)).get();
                        successCount++;

                        if (successCount % 1000 == 0) {
                            log.info("📊 Progress: {} successful, {} errors", successCount, errorCount);
                        }

                    } catch (Exception e) {
                        errorCount++;
                        log.error("❌ Failed to publish product {}: {}",
                                product.getProductId(), e.getMessage());

                        // ЛОГИРУЙ ПРОБЛЕМНЫЙ ПРОДУКТ ДЛЯ ДИАГНОСТИКИ
                        logProblematicProduct(product, e);
                    }
                }

                log.info("🎉 Batch completed: {} successful, {} errors, {} total",
                        successCount, errorCount, products.size());

                JsonFileManager.moveFile(path, readyDir);

            } catch (Exception e) {
                log.error("💥 Error in publishing loop: {}", e.getMessage());
            }
        }
    }

    private static boolean isValidProduct(ProductDTO product) {
        if (product == null) {
            log.warn("❌ Product is null");
            return false;
        }

        if (product.getProductId() == null || product.getProductId().trim().isEmpty()) {
            log.warn("❌ Product ID is null or empty");
            return false;
        }

        if (product.getName() == null || product.getName().trim().isEmpty()) {
            log.warn("❌ Product name is null or empty: {}", product.getProductId());
            return false;
        }

        if (product.getPrice() == null) {
            log.warn("❌ Product price is null: {}", product.getProductId());
            return false;
        }

        if (product.getPrice().getAmount() == null) {
            log.warn("❌ Product price amount is null: {}", product.getProductId());
            return false;
        }

        if (product.getPrice().getCurrency() == null) {
            log.warn("❌ Product price currency is null: {}", product.getProductId());
            return false;
        }

        if (product.getStock() == null) {
            log.warn("❌ Product stock is null: {}", product.getProductId());
            return false;
        }

        if (product.getStock().getAvailable() == null) {
            log.warn("❌ Product stock available is null: {}", product.getProductId());
            return false;
        }

        if (product.getStock().getReserved() == null) {
            log.warn("❌ Product stock reserved is null: {}", product.getProductId());
            return false;
        }

        return true;
    }

    private static void logProblematicProduct(ProductDTO product, Exception e) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);

            String productJson = mapper.writeValueAsString(product);
            log.error("🔍 Problematic product data:\n{}", productJson);

            // ЛОГИРУЙ КОНКРЕТНЫЕ ПОЛЯ
            log.error("📋 Product details - ID: {}, Name: {}, Price: {}, Stock: {}",
                    product.getProductId(),
                    product.getName(),
                    product.getPrice() != null ?
                            product.getPrice().getAmount() + " " + product.getPrice().getCurrency() : "null",
                    product.getStock() != null ?
                            product.getStock().getAvailable() + "/" + product.getStock().getReserved() : "null");

        } catch (Exception jsonError) {
            log.error("❌ Cannot serialize problematic product: {}", jsonError.getMessage());
        }
    }
}
