package ru.practicum.shopProductFilterStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import ru.practicum.common.config.KafkaProperties;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class shopProductFilterStreamApplacation {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Set<String> blockedProductNames = new HashSet<>();

    public static void main(String[] args) {
        startStreamsProcessing();
    }

    private static void startStreamsProcessing() {
        Properties props = KafkaProperties.getStreamsConfig();


        StreamsBuilder builder = new StreamsBuilder();

        // Настройка KafkaJsonSchemaSerde
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", KafkaProperties.getSchemaRegistryUrl());
        serdeConfig.put("basic.auth.credentials.source", "URL");
        // Добавьте другие настройки безопасности если нужно
        serdeConfig.put("schema.registry.ssl.truststore.location", "");
        serdeConfig.put("schema.registry.ssl.truststore.password", "");




        // Обработка заблокированных продуктов с JSON Schema Serde
        builder.stream(KafkaProperties.getTopicBlockedProducts(),
                        Consumed.with(Serdes.String(), Serdes.String()))
                .foreach((key, value) -> {
                    System.out.println("🔍 PROCESSING BLOCKED PRODUCT - Key: '" + key + "'");

                    try {
                        String productName = null;

                        // Способ 1: Используем ключ (как в вашем примере)
                        if (key != null && !key.trim().isEmpty()) {
                            productName = key.trim();
                            System.out.println("📝 Using KEY as product name: " + productName);
                        }
                        // Способ 2: Используем значение (JSON объект)
                        else if (value != null) {
                            productName = extractNameFromJsonSchemaValue(value);
                            if (productName != null) {
                                System.out.println("📝 Using VALUE as product name: " + productName);
                            }
                        }

                        if (productName != null && !productName.isEmpty()) {
                            blockedProductNames.add(productName);
                            System.out.println("🚫 BLOCKED PRODUCT ADDED: " + productName +
                                    ", Total blocked: " + blockedProductNames.size());
                        } else {
                            System.err.println("❌ Cannot extract product name");
                            debugJsonSchemaValue(value);
                        }

                    } catch (Exception e) {
                        System.err.println("💥 ERROR processing blocked product: " + e.getMessage());
                        e.printStackTrace();
                    }
                });

        // Обработка основных продуктов с JSON Schema Serde
        builder.stream("shopTopic", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, value) -> {
                    System.out.println(">>> NEW PRODUCT ARRIVED <<<");
                    System.out.println("Blocked products count: " + blockedProductNames.size());
                })
                .filter((key, value) -> {
                    try {
                        // Проверка на null
                        if (value == null || value.trim().isEmpty()) {
                            System.out.println("⏩ Skipping null/empty value");
                            return false;
                        }

                        // Очистка и подготовка данных
                        String cleanedValue = cleanInput(value);
                        if (cleanedValue == null) {
                            System.out.println("⏩ Skipping - cannot clean input data");
                            return false;
                        }

                        System.out.println("Cleaned data: " + cleanedValue);

                        // Парсинг JSON
                        JsonNode product;
                        try {
                            product = mapper.readTree(cleanedValue);
                        } catch (Exception e) {
                            System.err.println("❌ JSON parsing failed: " + e.getMessage());
                            return false;
                        }

                        // Проверка обязательных полей
                        if (!product.has("name")) {
                            System.out.println("⏩ Skipping product without 'name' field");
                            return false;
                        }

                        String productName = product.get("name").asText();
                        System.out.println("Product name: " + productName);

                        // Проверка на блокировку
                        boolean isBlocked = blockedProductNames.contains(productName);

                        if (!isBlocked) {
                            System.out.println("🎉 PRODUCT ALLOWED: " + productName);
                            return true;
                        } else {
                            System.out.println("🚷 PRODUCT BLOCKED: " + productName);
                            return false;
                        }

                    } catch (Exception e) {
                        System.err.println("💥 FILTER ERROR: " + e.getMessage());
                        System.err.println("Raw data: " + (value != null ?
                                value.substring(0, Math.min(value.length(), 100)) : "null"));
                        return false;
                    }
                })
                .peek((key, value) -> {
                    System.out.println("💾 SENDING TO OUTPUT TOPIC");
                })
                .to("products", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Добавьте listener для отслеживания состояния
        streams.setStateListener((newState, oldState) -> {
            System.out.println("🔄 STATE CHANGE: " + oldState + " -> " + newState);
        });

        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            System.err.println("💥 UNCAUGHT EXCEPTION in " + thread + ": " + throwable.getMessage());
            throwable.printStackTrace();
        });

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("🛑 Shutting down streams application...");
                streams.close();
                latch.countDown();
                System.out.println("✅ Streams application closed");
            }
        });

        try {
            streams.start();
            System.out.println("✅ Streams application STARTED successfully with JSON Schema Serde");
            System.out.println("📊 Waiting for messages...");
            latch.await();
        } catch (final Throwable e) {
            System.err.println("❌ Error starting streams: " + e.getMessage());
            System.exit(1);
        }
    }

    private static String extractNameFromJsonSchemaValue(Object value) {
        if (value == null) return null;

        try {
            // Значение может быть Map или кастомным объектом
            if (value instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) value;
                Object nameObj = map.get("name");
                return nameObj != null ? nameObj.toString() : null;
            } else {
                // Если это кастомный объект, используем рефлексию или преобразуем в Map
                String jsonString = mapper.writeValueAsString(value);
                JsonNode jsonNode = mapper.readTree(jsonString);
                return jsonNode.has("name") ? jsonNode.get("name").asText() : null;
            }
        } catch (Exception e) {
            System.err.println("Error extracting name from JSON Schema value: " + e.getMessage());
            return null;
        }
    }

    private static void debugJsonSchemaValue(Object value) {
        if (value == null) {
            System.out.println("JSON Schema value: null");
            return;
        }

        System.out.println("=== JSON SCHEMA VALUE DEBUG ===");
        System.out.println("Value type: " + value.getClass().getName());

        try {
            String jsonString = mapper.writeValueAsString(value);
            System.out.println("As JSON: " + jsonString);

            if (value instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) value;
                System.out.println("Map contents:");
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    System.out.println("  " + entry.getKey() + ": " + entry.getValue());
                }
            }
        } catch (Exception e) {
            System.err.println("Error debugging value: " + e.getMessage());
        }
        System.out.println("===============================");
    }

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
                String possibleJson = cleaned.substring(5); // Пропускаем magic byte + schema ID
                if (possibleJson.startsWith("{") && possibleJson.endsWith("}")) {
                    return possibleJson;
                }
            }

            return null;
        } catch (Exception e) {
            return null;
        }
    }
}

