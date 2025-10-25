package ru.practicum.client.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class RecommendationConsumer {
    private final ObjectMapper objectMapper;
    private final Map<String, UserRecommendation> recommendations = new ConcurrentHashMap<>();
    private volatile boolean isInitialized = false;
    private volatile boolean isRunning = true;

    @Data
    public static class UserRecommendation {
        @JsonProperty("RECOMMENDATION_TEXT")
        private String recommendationText;

        @JsonProperty("RECOMMENDATION_TYPE")
        private String recommendationType;

        @JsonProperty("RECOMMENDATION_MESSAGE")
        private String recommendationMessage;

        @JsonProperty("LAST_RESULTS_COUNT")
        private Integer lastResultsCount;

        // Геттеры для обратной совместимости
        public String getRecommendation_text() {
            return recommendationText;
        }

        public String getRecommendation_type() {
            return recommendationType;
        }

        public String getRecommendation_message() {
            return recommendationMessage;
        }
    }

    @PostConstruct
    public void start() {
        log.info("🚀 Starting RecommendationConsumer...");

        Thread consumerThread = new Thread(() -> {
            Properties props = createConsumerProperties();

            try (KafkaConsumer<byte[], String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of("user_recommendations"));
                log.info("✅ Subscribed to topic: user_recommendations");

                // Ждем присвоения партиций
                int attempts = 0;
                while (consumer.assignment().isEmpty() && attempts < 10 && isRunning) {
                    consumer.poll(Duration.ofMillis(100));
                    attempts++;
                    log.debug("Waiting for partition assignment... {}/10", attempts);
                }

                if (!consumer.assignment().isEmpty()) {
                    // Переходим в начало всех партиций чтобы прочитать все сообщения
                    consumer.seekToBeginning(consumer.assignment());
                    log.info("✅ Seek to beginning completed for partitions: {}", consumer.assignment());
                } else {
                    log.warn("⚠️ No partitions assigned to consumer");
                }

                isInitialized = true;
                log.info("✅ Consumer initialized and ready to poll messages");

                // Основной цикл опроса
                while (isRunning) {
                    try {
                        ConsumerRecords<byte[], String> records = consumer.poll(Duration.ofMillis(1000));

                        if (!records.isEmpty()) {
                            log.info("📨 Polled {} records from Kafka", records.count());

                            for (ConsumerRecord<byte[], String> record : records) {
                                processRecommendation(record.key(), record.value());
                            }

                            // Коммитим оффсеты после обработки
                            consumer.commitSync();
                            log.debug("✅ Committed offsets successfully");
                        }
                    } catch (Exception e) {
                        log.error("❌ Error during poll cycle", e);
                    }
                }

            } catch (Exception e) {
                log.error("❌ Fatal error in RecommendationConsumer", e);
                isInitialized = false;
            } finally {
                log.info("🛑 RecommendationConsumer stopped");
                isInitialized = false;
            }
        });

        consumerThread.setName("RecommendationConsumer-Thread");
        consumerThread.setDaemon(true);
        consumerThread.start();

        log.info("✅ RecommendationConsumer startup initiated");
    }

    private void processRecommendation(byte[] keyBytes, String json) {
        try {
            // Конвертируем бинарный ключ в строку
            String userId = convertKeyToString(keyBytes);

            log.info("📥 Received recommendation for user {}: {}", userId, json);

            UserRecommendation recommendation = objectMapper.readValue(json, UserRecommendation.class);
            recommendations.put(userId, recommendation);

            String message = recommendation.getRecommendationMessage() != null ?
                    recommendation.getRecommendationMessage() :
                    (recommendation.getRecommendationText() != null ?
                            "Search: " + recommendation.getRecommendationText() : "No message");

            log.info("✅ Processed recommendation for user {}: {}", userId, message);

        } catch (Exception e) {
            log.error("❌ Failed to parse recommendation JSON for key {}: {}",
                    keyBytes != null ? bytesToHex(keyBytes) : "null", json, e);
        }
    }

    private String convertKeyToString(byte[] keyBytes) {
        if (keyBytes == null) {
            return "unknown";
        }

        try {
            // Для ключа 5 (0x05) - это один байт со значением 5
            if (keyBytes.length == 1) {
                int userId = keyBytes[0];
                // Если это отрицательное число (в случае signed byte), преобразуем
                if (userId < 0) {
                    userId = userId & 0xFF;
                }
                log.debug("Converted binary key {} to user ID: {}", bytesToHex(keyBytes), userId);
                return String.valueOf(userId);
            } else {
                String result = new String(keyBytes).trim();
                log.debug("Converted string key {} to user ID: {}", bytesToHex(keyBytes), result);
                return result;
            }
        } catch (Exception e) {
            String result = new String(keyBytes).trim();
            log.warn("Fallback conversion for key {} to: {}", bytesToHex(keyBytes), result);
            return result;
        }
    }

    private String bytesToHex(byte[] bytes) {
        if (bytes == null) return "null";
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }

    public UserRecommendation getRecommendation(Long userId) {
        if (!isInitialized) {
            log.warn("⚠️ Consumer not initialized yet for user: {}", userId);
            return null;
        }

        // Пробуем разные варианты ключа
        String[] possibleKeys = {
                userId.toString(),
                String.valueOf(userId.intValue()),
                convertKeyToString(new byte[] { userId.byteValue() })
        };

        for (String key : possibleKeys) {
            UserRecommendation rec = recommendations.get(key);
            if (rec != null) {
                log.info("✅ Found recommendation for user {} using key '{}': {}", userId, key,
                        rec.getRecommendationMessage() != null ? rec.getRecommendationMessage() : rec.getRecommendationText());
                return rec;
            }
        }

        log.warn("❌ No recommendation found in cache for user: {}. Tried keys: {}", userId, List.of(possibleKeys));
        log.info("📊 Current cache contents: {} items", recommendations.size());
        recommendations.forEach((k, v) -> log.info("   User '{}': {}", k,
                v.getRecommendationMessage() != null ? v.getRecommendationMessage() : v.getRecommendationText()));
        return null;
    }

    public UserRecommendation getRecommendation(String userId) {
        return recommendations.get(userId);
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    public void waitForInitialization() {
        int attempts = 0;
        while (!isInitialized && attempts < 30 && isRunning) {
            try {
                Thread.sleep(1000);
                attempts++;
                if (attempts % 5 == 0) {
                    log.info("⏳ Waiting for consumer initialization... {}/30", attempts);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (!isInitialized) {
            log.error("❌ Consumer failed to initialize within 30 seconds");
        } else {
            log.info("✅ Consumer initialization completed successfully");
        }
    }

    public void stop() {
        isRunning = false;
        log.info("🛑 Stopping RecommendationConsumer...");
    }

    private Properties createConsumerProperties() {
        Properties props = new Properties();

        // Основные настройки
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:1090,kafka-1:2090");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-recommendation-consumer-v2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Ручной коммит для надежности
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");

        // Безопасность
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";");
        props.put("ssl.truststore.location", "/etc/kafka/secrets/kafka.truststore.jks");
        props.put("ssl.truststore.password", "password");
        props.put("ssl.keystore.location", "/etc/kafka/secrets/kafka.keystore.pkcs12");
        props.put("ssl.keystore.password", "password");
        props.put("ssl.key.password", "password");
        props.put("ssl.endpoint.identification.algorithm", "");

        log.info("✅ Kafka consumer properties configured");
        return props;
    }

    // Метод для отладки - показывает все текущие рекомендации
    public void printAllRecommendations() {
        log.info("📊 Current recommendations cache ({} items):", recommendations.size());
        if (recommendations.isEmpty()) {
            log.info("📊 No recommendations in cache");
        } else {
            recommendations.forEach((userId, rec) -> {
                String message = rec.getRecommendationMessage() != null ?
                        rec.getRecommendationMessage() :
                        (rec.getRecommendationText() != null ?
                                "Search: " + rec.getRecommendationText() : "No message");
                log.info("   User '{}': {}", userId, message);
            });
        }
    }

    public int getRecommendationsCount() {
        return recommendations.size();
    }

    // Метод для получения всех рекомендаций (для отладки)
    public Map<String, UserRecommendation> getAllRecommendations() {
        return new ConcurrentHashMap<>(recommendations);
    }

    // Метод для очистки кэша (для тестирования)
    public void clearCache() {
        int size = recommendations.size();
        recommendations.clear();
        log.info("🧹 Cleared recommendation cache ({} items removed)", size);
    }

    // Метод для проверки наличия рекомендации для пользователя
    public boolean hasRecommendation(Long userId) {
        String[] possibleKeys = {
                userId.toString(),
                String.valueOf(userId.intValue()),
                convertKeyToString(new byte[] { userId.byteValue() })
        };

        for (String key : possibleKeys) {
            if (recommendations.containsKey(key)) {
                return true;
            }
        }
        return false;
    }
}