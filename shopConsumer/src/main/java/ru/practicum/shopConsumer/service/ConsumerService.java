package ru.practicum.shopConsumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.stereotype.Service;
import ru.practicum.common.config.KafkaProperties;
import ru.practicum.common.dto.ProductDTO;
import ru.practicum.common.model.Product;
import ru.practicum.common.mapper.ProductMapper;
import ru.practicum.common.repository.ProductRepository;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;


@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerService {

    private final ProductRepository productRepository;
    private final ProductMapper productMapper;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread consumerThread;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        log.info("🔄 Initializing Kafka Consumer Service...");
        startConsumer();
    }

    @PreDestroy
    public void cleanup() {
        log.info("🛑 Shutting down Kafka Consumer Service...");
        stopConsumer();
    }

    public void startConsumer() {
        if (consumerThread != null && consumerThread.isAlive()) {
            log.warn("⚠️ Consumer is already running");
            return;
        }

        consumerThread = new Thread(this::runConsumer);
        consumerThread.setName("kafka-product-consumer");
        consumerThread.setDaemon(false); // Важно: не daemon thread для Docker
        consumerThread.start();

        log.info("🚀 Kafka Product Consumer started in thread: {}", consumerThread.getName());
    }

    public void stopConsumer() {
        log.info("🛑 Stopping Kafka Consumer...");
        running.set(false);

        if (consumerThread != null) {
            try {
                consumerThread.join(10000); // Ждем завершения до 10 секунд
                log.info("✅ Kafka Consumer stopped gracefully");
            } catch (InterruptedException e) {
                log.warn("⚠️ Consumer stop interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }

    public void runConsumer() {
        Properties props = KafkaProperties.getConsumerProperties("product-consumer-group");

        try (KafkaConsumer<String, ProductDTO> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(KafkaProperties.getProductsTopicName()));
            log.info("✅ Consumer subscribed to topic: {}", KafkaProperties.getProductsTopicName());

            while (running.get()) {
                try {
                    ConsumerRecords<String, ProductDTO> records = consumer.poll(Duration.ofMillis(1000));

                    if (!records.isEmpty()) {
                        log.info("📥 Received {} messages from Kafka", records.count());

                        for (ConsumerRecord<String, ProductDTO> record : records) {
                            try {
                                // Данные УЖЕ десериализованы в ProductDTO!
                                ProductDTO productDTO = record.value();
                                Product product = productMapper.toEntity(productDTO);
                                validateProduct(product);

                                productRepository.save(product);
                                log.info("✅ Processed product: {} - {}",
                                        product.getProductId(), product.getName());

                            } catch (Exception e) {
                                log.error("❌ Failed to process product: {}", e.getMessage(), e);
                            }
                        }

                        consumer.commitSync();
                    }

                } catch (Exception e) {
                    log.error("💥 Error during message processing: {}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("❌ Fatal error in consumer: {}", e.getMessage(), e);
        }
    }

    private boolean processAndSaveRecords(ConsumerRecords<String, String> records) {
        try {
            List<Product> productsToSave = new ArrayList<>();
            int processedCount = 0;
            int errorCount = 0;

            for (ConsumerRecord<String, String> record : records) {
                try {
                    // Парсим JSON строку в ProductDTO
                    ProductDTO productDTO = parseJsonToProductDTO(record.value());

                    // Преобразуем в Entity
                    Product product = productMapper.toEntity(productDTO);
                    validateProduct(product);

                    productsToSave.add(product);
                    processedCount++;

                    log.debug("✅ Parsed product: {} - {}", product.getProductId(), product.getName());

                } catch (Exception e) {
                    errorCount++;
                    log.error("""
                            💥 Failed to parse record:
                               Offset: {}
                               Partition: {}
                               Key: {}
                               Value: {}
                               Error: {}
                            """,
                            record.offset(),
                            record.partition(),
                            record.key(),
                            record.value().substring(0, Math.min(record.value().length(), 200)), // Логируем часть значения
                            e.getMessage()
                    );
                }
            }

            // Сохраняем в БД
            if (!productsToSave.isEmpty()) {
                List<Product> savedProducts = productRepository.saveAll(productsToSave);
                log.info("💾 Successfully saved {} products (processed: {}, errors: {})",
                        savedProducts.size(), processedCount, errorCount);
                return true;
            } else {
                log.warn("⚠️ No valid products to save (processed: {}, errors: {})",
                        processedCount, errorCount);
                return errorCount == 0; // Если только ошибки - считаем неудачей
            }

        } catch (Exception e) {
            log.error("❌ Batch processing failed: {}", e.getMessage(), e);
            return false;
        }
    }

    private ProductDTO parseJsonToProductDTO(String json) throws Exception {
        try {
            // Удаляем бинарные символы из начала строки
            String cleanedJson = removeBinaryPrefix(json);

            // Проверяем, что после очистки у нас валидный JSON
            if (!isValidJson(cleanedJson)) {
                throw new IllegalArgumentException("Invalid JSON format after cleaning");
            }

            return objectMapper.readValue(cleanedJson, ProductDTO.class);
        } catch (Exception e) {
            log.error("❌ JSON parsing error. Original content: {}",
                    escapeNonPrintable(json.substring(0, Math.min(json.length(), 200))));
            throw e;
        }
    }

    private String removeBinaryPrefix(String json) {
        if (json == null || json.isEmpty()) {
            return json;
        }

        // Ищем позицию начала JSON (первая фигурная скобка)
        int jsonStart = json.indexOf('{');
        if (jsonStart > 0) {
            // Обрезаем все до первой фигурной скобки
            return json.substring(jsonStart);
        }

        // Альтернативный способ: удаляем все не-печатные символы из начала
        String cleaned = json.replaceAll("^[\\x00-\\x1F\\x7F]+", "");
        if (!cleaned.isEmpty()) {
            return cleaned;
        }

        return json;
    }

    private boolean isValidJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            return false;
        }

        String trimmed = json.trim();
        return trimmed.startsWith("{") && trimmed.endsWith("}");
    }

    private String escapeNonPrintable(String text) {
        if (text == null) return null;

        StringBuilder sb = new StringBuilder();
        for (char c : text.toCharArray()) {
            if (c < 32 || c == 127) {
                sb.append(String.format("\\u%04x", (int) c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private void validateProduct(Product product) {
        if (product.getProductId() == null || product.getProductId().trim().isEmpty()) {
            throw new IllegalArgumentException("Product ID cannot be null or empty");
        }

        if (product.getName() == null || product.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("Product name cannot be null or empty");
        }


        if (product.getCategory() == null || product.getCategory().trim().isEmpty()) {
            throw new IllegalArgumentException("Product category cannot be null or empty");
        }
    }
}
