package ru.practicum.shopConsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan("ru.practicum.common.model")
@EnableJpaRepositories("ru.practicum.common.repository")
// Добавьте явное сканирование для mapper пакета
@ComponentScan({
        "ru.practicum.shopConsumer",
        "ru.practicum.common.repository",
        "ru.practicum.common.mapper",
        "ru.practicum.common.config"
})
public class ShopConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ShopConsumerApplication.class, args);
    }
}