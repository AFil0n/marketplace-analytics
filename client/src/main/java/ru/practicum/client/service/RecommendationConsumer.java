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
    }

    @PostConstruct
    public void start() {
        log.info("🚀 Starting RecommendationConsumer...");

        Thread consumerThread = new Thread(() -> {
            Properties props = createConsumerProperties();

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of("user_recommendations"));
                log.info("✅ Subscribed to topic: user_recommendations");

                // Основной цикл опроса
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> record : records) {
                        String userId = record.key(); // Теперь это строка "5"
                        String json = record.value();

                        try {
                            UserRecommendation recommendation = objectMapper.readValue(json, UserRecommendation.class);
                            recommendations.put(userId, recommendation);

                            log.info("✅ Processed recommendation for user {}: {}", userId,
                                    recommendation.getRecommendationMessage() != null ?
                                            recommendation.getRecommendationMessage() : "No message");

                        } catch (Exception e) {
                            log.error("❌ Failed to parse recommendation for user {}: {}", userId, json, e);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("❌ Consumer error", e);
            }
        });

        consumerThread.setName("RecommendationConsumer-Thread");
        consumerThread.setDaemon(true);
        consumerThread.start();

        log.info("✅ RecommendationConsumer started successfully");
    }

    public UserRecommendation getRecommendation(Long userId) {
        String key = userId.toString();
        UserRecommendation rec = recommendations.get(key);

        if (rec != null) {
            log.info("✅ Found recommendation for user {}: {}", userId, rec.getRecommendationMessage());
        } else {
            log.warn("❌ No recommendation found for user: {}", userId);
            log.info("📊 Available users in cache: {}", recommendations.keySet());
        }

        return rec;
    }

    private Properties createConsumerProperties() {
        Properties props = new Properties();

        // Основные настройки
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:1090,kafka-1:2090");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-recommendation-consumer-simple");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

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
}