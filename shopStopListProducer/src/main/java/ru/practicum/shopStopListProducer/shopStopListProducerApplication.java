package ru.practicum.shopStopListProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.practicum.common.config.KafkaProperties;

@SpringBootApplication
public class ShopStopListProducerApplication {
    private static final String STOP_LIST_PATH = "/etc/data/stoplist.txt";

    public static void main(String[] args) {
        SpringApplication.run(ShopStopListProducerApplication.class, args);
    }


}
