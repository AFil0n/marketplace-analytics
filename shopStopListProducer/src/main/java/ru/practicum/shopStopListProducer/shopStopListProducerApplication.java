package ru.practicum.shopStopListProducer;

import com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.serialization.StringSerializer;
import ru.practicum.common.config.KafkaProperties;

public class shopStopListProducerApplication {
    private static final String STOP_LIST_PATH = "/etc/data/stoplist.txt";

    public static void main(String[] args) {
        runShopStopListProducer();
    }

    private static void runShopStopListProducer() {
        Properties props = KafkaProperties.getProducerProperties();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = new BufferedReader(new FileReader(STOP_LIST_PATH))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String name = line.trim().replace(",", "");

                if (!name.isEmpty()) {
                    String jsonMessage = String.format("{\"name\": \"%s\"}", name);

                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(KafkaProperties.getTopicBlockedProducts(), name, jsonMessage);

                    try {
                        producer.send(record).get(); // Ждем подтверждения
                        System.out.println("Successfully sent blocked product: " + name);
                    } catch (InterruptedException | ExecutionException e) {
                        System.err.println("Error sending product " + name + ": " + e.getMessage());
                    }
                }
            }

            producer.flush();
            System.out.println("All blocked products from file sent to Kafka topic: " + KafkaProperties.getTopicBlockedProducts());

        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
    }
}
