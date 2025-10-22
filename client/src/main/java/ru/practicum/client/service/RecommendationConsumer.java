package ru.practicum.client.service;

import com.fasterxml.jackson.databind.ObjectMapper;
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
        private String recommendation_text;
        private String recommendation_type;
        private String recommendation_message;
        private String last_query_time;
    }

    @PostConstruct
    public void start() {
        Properties props = createConsumerProperties();

        Thread consumerThread = new Thread(() -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of("user_recommendations"));
                log.info("✅ JSON recommendation consumer started");

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> record : records) {
                        processRecommendation(record.key(), record.value());
                    }
                }
            } catch (Exception e) {
                log.error("❌ Consumer error", e);
            }
        });
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    private void processRecommendation(String userId, String json) {
        try {
            UserRecommendation recommendation = objectMapper.readValue(json, UserRecommendation.class);
            recommendations.put(userId, recommendation);

            log.info("✅ Processed recommendation for {}: {}", userId, recommendation.getRecommendation_message());

        } catch (Exception e) {
            log.error("❌ Failed to parse recommendation JSON: {}", json, e);
        }
    }

    public UserRecommendation getRecommendation(String userId) {
        return recommendations.get(userId);
    }

    private Properties createConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0:1090,kafka-1:2090");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-recommendation-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

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

        return props;
    }
}
