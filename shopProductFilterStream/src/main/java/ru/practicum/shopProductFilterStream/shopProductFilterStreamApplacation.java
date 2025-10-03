package ru.practicum.shopProductFilterStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import ru.practicum.common.config.KafkaProperties;

import java.util.HashSet;
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-filter-app");

        // Добавляем настройки для лучшей обработки ошибок
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.DefaultProductionExceptionHandler");

        StreamsBuilder builder = new StreamsBuilder();

        // Подписываемся на топик с заблокированными продуктами
        builder.stream(KafkaProperties.getTopicBlockedProducts(),
                        Consumed.with(Serdes.String(), Serdes.String()))
                .foreach((key, value) -> {
                    try {
                        if (value != null) {
                            JsonNode blockedProduct = mapper.readTree(value);
                            String productName = blockedProduct.get("name").asText();
                            blockedProductNames.add(productName);
                            System.out.println("✅ Blocked product added: " + productName +
                                    ", total blocked: " + blockedProductNames.size());
                        }
                    } catch (Exception e) {
                        System.err.println("Error parsing blocked product JSON: " + e.getMessage());
                    }
                });

        // Обрабатываем основной поток продуктов
        builder.stream(KafkaProperties.getShopProducerTopicName(),
                        Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> {
                    try {
                        if (value == null) {
                            System.out.println("Skipping null value");
                            return false;
                        }

                        JsonNode product = mapper.readTree(value);
                        if (!product.has("product_id")) {
                            System.out.println("Skipping product without product_id");
                            return false;
                        }

                        String productName = product.get("name").asText();
                        boolean isBlocked = blockedProductNames.contains(productName);

                        if (!isBlocked) {
                            System.out.println("Product allowed: " + productName);
                            return true;
                        } else {
                            System.out.println("Product blocked: " + productName);
                            return false;
                        }
                    } catch (Exception e) {
                        System.err.println("Error parsing product JSON: " + e.getMessage());
                        return false;
                    }
                })
                .to(KafkaProperties.getProductsTopicName(), Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        final CountDownLatch latch = new CountDownLatch(1);

        // Добавляем обработчик завершения работы
        Runtime.getRuntime().addShutdownHook(new Thread("product-filter-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Shutting down streams application...");
                streams.close();
                latch.countDown();
                System.out.println("Streams application closed");
            }
        });

        try {
            streams.start();
            System.out.println("Streams application started");
            latch.await();
        } catch (final Throwable e) {
            System.err.println("Error starting streams application: " + e.getMessage());
            System.exit(1);
        }
    }
}

