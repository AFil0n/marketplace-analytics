package ru.practicum.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.practicum.common.services.ProductService;
import ru.practicum.common.services.RecommendationService;

import java.util.List;
import java.util.Scanner;

@Slf4j
@SpringBootApplication
public class ClientApplication {
    public static void main(String[] args) {
        SpringApplication.run(ClientApplication.class, args);
    }
}
