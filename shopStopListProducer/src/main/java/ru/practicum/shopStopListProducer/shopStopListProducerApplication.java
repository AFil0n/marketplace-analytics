package ru.practicum.shopStopListProducer;

import java.util.Properties;

public class shopStopListProducerApplication {
    private static final String STOP_LIST = "/etc/data";

    public static void main(String[] args) {
        runShopStopListProducer();
    }

    private static void runShopStopListProducer() {
        Properties props = KafkaProperties.getProducerProperties();
    }
}
