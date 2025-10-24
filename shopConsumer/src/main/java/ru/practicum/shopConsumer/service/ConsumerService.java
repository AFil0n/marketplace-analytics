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
import org.springframework.transaction.annotation.Transactional;
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
                                Thread.sleep(50L);
                                ProductDTO productDTO = record.value();
                                Product product = productMapper.toEntity(productDTO);
                                validateProduct(product);

                                saveProductWithTransaction(product);

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

    @Transactional
    public void saveProductWithTransaction(Product product) {
        Product savedProduct = productRepository.saveAndFlush(product);
        log.info("💾 TRANSACTIONAL SAVE: Product ID {} saved with DB ID {}",
                savedProduct.getProductId(), savedProduct.getProductId());
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
