package ru.practicum.shopProductFilterStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serdes;
import ru.practicum.common.config.KafkaProperties;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class ShopProductFilterStreamApplication {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final AtomicLong blockedProductsCounter = new AtomicLong(0);

    public static void main(String[] args) {
        startStreamsProcessing();
    }

    private static void startStreamsProcessing() {
        Properties props = KafkaProperties.getStreamsConfig();
        StreamsBuilder builder = new StreamsBuilder();

        // Настройка сериализации
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", KafkaProperties.getSchemaRegistryUrl());
        serdeConfig.put("basic.auth.credentials.source", "URL");
        serdeConfig.put("schema.registry.ssl.truststore.location", "");
        serdeConfig.put("schema.registry.ssl.truststore.password", "");

        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Создаем KTable для заблокированных продуктов
        KTable<String, String> blockedProductsTable = builder.table(
                KafkaProperties.getTopicBlockedProducts(),
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("blocked-products-store")
        );

        // Логирование изменений в таблице заблокированных продуктов с подсчетом
        blockedProductsTable.toStream()
                .peek((key, value) -> {
                    if (value == null) {
                        System.out.println("🗑️ BLOCKED PRODUCT REMOVED: " + key);
                        blockedProductsCounter.decrementAndGet();
                    } else {
                        System.out.println("➕ BLOCKED PRODUCT ADDED/UPDATED: " + key);
                        blockedProductsCounter.incrementAndGet();
                        try {
                            String productName = extractProductName(key, value);
                            System.out.println("   ProductDTO details: " + productName);
                        } catch (Exception e) {
                            System.err.println("   Error parsing product details: " + e.getMessage());
                        }
                    }
                    System.out.println("📊 CURRENT BLOCKED PRODUCTS COUNT: " + blockedProductsCounter.get());
                });

        // Обработка основных продуктов
        builder.stream("shopTopic", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, value) -> {
                    System.out.println(">>> NEW PRODUCT ARRIVED <<<");
                    System.out.println("   Key: " + key);
                    System.out.println("   Value preview: " + (value != null ?
                            value.substring(0, Math.min(value.length(), 200)) : "null"));
                    System.out.println("   Current blocked products: " + blockedProductsCounter.get());
                })
                .filter((key, value) -> {
                    // Первичная фильтрация невалидных данных
                    if (value == null || value.trim().isEmpty()) {
                        System.out.println("⏩ Skipping null/empty value");
                        return false;
                    }

                    String cleanedValue = cleanInput(value);
                    if (cleanedValue == null) {
                        System.out.println("⏩ Skipping - cannot clean input data");
                        return false;
                    }

                    return true;
                })
                .leftJoin(blockedProductsTable,
                        (productValue, blockedProductValue) -> {
                            try {
                                System.out.println("🔍 JOINING PRODUCT WITH BLOCKED LIST");

                                String cleanedValue = cleanInput(productValue);
                                JsonNode product = mapper.readTree(cleanedValue);

                                // Извлекаем имя продукта для проверки
                                String productName = extractProductNameFromJson(product);
                                if (productName == null) {
                                    System.out.println("⏩ Skipping product without valid name");
                                    return new ProcessingResult(null, false, "No valid name");
                                }

                                System.out.println("   Checking product: " + productName);

                                // Продукт заблокирован если есть запись в blockedProductsTable
                                boolean isBlocked = (blockedProductValue != null);

                                if (isBlocked) {
                                    System.out.println("🚷 PRODUCT BLOCKED: " + productName);
                                    System.out.println("   Blocked data: " + blockedProductValue);
                                    return new ProcessingResult(null, false, "Blocked");
                                } else {
                                    System.out.println("🎉 PRODUCT ALLOWED: " + productName);
                                    return new ProcessingResult(productValue, true, "Allowed");
                                }

                            } catch (Exception e) {
                                System.err.println("💥 JOIN ERROR: " + e.getMessage());
                                return new ProcessingResult(null, false, "Processing error: " + e.getMessage());
                            }
                        })
                .filter((key, result) -> {
                    // Фильтруем заблокированные продукты
                    boolean allowed = result != null && result.isAllowed();
                    System.out.println("🎯 FILTER DECISION: " + (allowed ? "PASS" : "REJECT") +
                            " - " + (result != null ? result.getReason() : "null result"));
                    return allowed;
                })
                .mapValues(ProcessingResult::getProductValue)
                .peek((key, value) -> {
                    try {
                        String cleanedValue = cleanInput(value);
                        JsonNode product = mapper.readTree(cleanedValue);
                        String productName = extractProductNameFromJson(product);
                        System.out.println("💾 SENDING TO OUTPUT TOPIC: " + productName);
                    } catch (Exception e) {
                        System.out.println("💾 SENDING TO OUTPUT TOPIC (raw value)");
                    }
                })
                .to("products", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Настройка listeners для мониторинга
        streams.setStateListener((newState, oldState) -> {
            System.out.println("🔄 KAFKA STREAMS STATE CHANGE: " + oldState + " -> " + newState);

            if (newState == KafkaStreams.State.RUNNING) {
                System.out.println("✅ Application is now RUNNING with KTable approach");
                System.out.println("💡 Blocked products are automatically updated from topic: " +
                        KafkaProperties.getTopicBlockedProducts());
            } else if (newState == KafkaStreams.State.REBALANCING) {
                System.out.println("⚡ Application is rebalancing...");
            }
        });

        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            System.err.println("💥 UNCAUGHT EXCEPTION in thread " + thread + ": " + throwable.getMessage());
            throwable.printStackTrace();
        });

        // Периодический вывод метрик (опционально)
        startMetricsLogger(streams);

        final CountDownLatch latch = new CountDownLatch(1);

        // Shutdown hook для graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("🛑 Shutting down streams application...");
                streams.close(Duration.ofSeconds(30));
                latch.countDown();
                System.out.println("✅ Streams application closed gracefully");
            }
        });

        try {
            streams.start();
            System.out.println("✅ Streams application STARTED successfully");
            System.out.println("📊 Application topology:");
            System.out.println(builder.build().describe());
            System.out.println("🚀 Waiting for messages...");
            latch.await();
        } catch (final Throwable e) {
            System.err.println("❌ Error starting streams application: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Запускает периодический вывод метрик
     */
    private static void startMetricsLogger(KafkaStreams streams) {
        Thread metricsThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(30000); // Каждые 30 секунд
                    System.out.println("📈 METRICS - Blocked products count: " + blockedProductsCounter.get());

                    // Дополнительные метрики из Kafka Streams
                    streams.metrics().forEach((name, metric) -> {
                        if (name.name().contains("blocked") || name.name().contains("record")) {
                            System.out.println("   " + name.name() + ": " + metric.metricValue());
                        }
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("Error logging metrics: " + e.getMessage());
                }
            }
        });
        metricsThread.setDaemon(true);
        metricsThread.start();
    }

    /**
     * Извлекает имя продукта из JSON объекта
     */
    private static String extractProductNameFromJson(JsonNode product) {
        if (product == null) return null;

        if (product.has("name")) {
            return product.get("name").asText().trim();
        }

        // Альтернативные поля, которые могут содержать имя
        String[] possibleNameFields = {"productName", "product", "title", "itemName"};
        for (String field : possibleNameFields) {
            if (product.has(field)) {
                return product.get(field).asText().trim();
            }
        }

        return null;
    }

    /**
     * Извлекает имя продукта из ключа или значения
     */
    private static String extractProductName(String key, String value) {
        // Приоритет: ключ -> значение
        if (key != null && !key.trim().isEmpty() && !"CLEAR".equals(key) && !"CLEAR_ALL".equals(key)) {
            return key.trim();
        }

        if (value != null) {
            try {
                JsonNode jsonNode = mapper.readTree(value);
                return extractProductNameFromJson(jsonNode);
            } catch (Exception e) {
                // Если не JSON, пытаемся извлечь как простую строку
                return value.trim();
            }
        }

        return null;
    }

    /**
     * Очищает входные данные
     */
    private static String cleanInput(String input) {
        if (input == null) return null;

        try {
            // Удаляем нулевые байты и контрольные символы
            String cleaned = input.replaceAll("[\\x00-\\x09\\x0B-\\x0C\\x0E-\\x1F\\x7F]", "").trim();

            // Проверяем базовую структуру JSON
            if (cleaned.startsWith("{") && cleaned.endsWith("}") && cleaned.length() > 2) {
                return cleaned;
            }

            // Если данные начинаются с magic byte (Avro), пробуем извлечь JSON
            if (cleaned.startsWith("\u0000") && cleaned.length() > 5) {
                String possibleJson = cleaned.substring(5);
                if (possibleJson.startsWith("{") && possibleJson.endsWith("}")) {
                    return possibleJson;
                }
            }

            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Вспомогательный класс для хранения результатов обработки
     */
    private static class ProcessingResult {
        private final String productValue;
        private final boolean allowed;
        private final String reason;

        public ProcessingResult(String productValue, boolean allowed, String reason) {
            this.productValue = productValue;
            this.allowed = allowed;
            this.reason = reason;
        }

        public String getProductValue() { return productValue; }
        public boolean isAllowed() { return allowed; }
        public String getReason() { return reason; }
    }
}