package ru.practicum.shopStopListProducer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;
import ru.practicum.common.config.KafkaProperties;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class StopListService {
    private final List<String> stopList;
    private static final String STOP_LIST_PATH = "/etc/data/stoplist.txt";

    public StopListService() {
        this.stopList = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(STOP_LIST_PATH))) {
            String line;

            while ((line = reader.readLine()) != null) {
                String name = line.trim().replace(",", "");
                this.stopList.add(name);
            }

        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
    }

    public String createStopListItem(String name) {
        this.stopList.add(name);
        Properties props = KafkaProperties.getProducerProperties();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Выносим создание сообщения до создания producer
        String jsonMessage = String.format("{\"name\": \"%s\"}", name);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(KafkaProperties.getTopicBlockedProducts(), name, jsonMessage);

            // Используем callback для асинхронной отправки
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Error sending message to Kafka: " + exception.getMessage(), exception);
                } else {
                    log.info("Successfully sent blocked product: {} to topic {}, partition {}, offset {}",
                            name, metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

        } catch (Exception e) {
            log.error("Error in Kafka producer: " + e.getMessage(), e);
            return "Error: " + e.getMessage();
        }

        return "Успешно добавлен '" + name + "' в кафка";
    }


    public String deleteStopListItem(String name) {
        Properties props = KafkaProperties.getProducerProperties();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String jsonMessage = String.format("{\"action\": \"delete\", \"name\": \"%s\"}", name);

        this.stopList.remove(name);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(KafkaProperties.getTopicBlockedProducts(), name, jsonMessage);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Error sending delete message to Kafka: " + exception.getMessage(), exception);
                } else {
                    log.info("Successfully sent delete request for product: {} to topic {}, partition {}, offset {}",
                            name, metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

        } catch (Exception e) {
            log.error("Error in Kafka producer while deleting: " + e.getMessage(), e);
            return "Error: " + e.getMessage();
        }

        return "Delete request for product '" + name + "' successfully sent to Kafka";
    }

    public List<String> getStopListItems() {
        return stopList;
    }

    public void runShopStopListProducer() {
        Properties props = KafkaProperties.getProducerProperties();

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for(String line : stopList){
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
        }
    }
}
