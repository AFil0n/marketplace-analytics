package ru.practicum.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.practicum.common.services.ProductService;
import ru.practicum.common.services.RecommendationService;

import java.util.List;
import java.util.Scanner;

@Slf4j
@SpringBootApplication
public class ClientApplication {
    private static final Scanner scanner = new Scanner(System.in);
    private static ProductService productService;
    private static RecommendationService recommendationService;

    public static void main(String[] args) {
        runMenu();
    }

    private static void runMenu() {
        log.info("🛒 Введите цифру для выбора команды");

        Integer userId = processUser();

        while (true) {
            log.info("🛒 Доступные команды:");
            log.info("1. Поиск <product_name> - поиск товара по имени");
            log.info("2. Рекомендации <user_id> - персонализированные рекомендации");
            log.info("3. Выход - выход из приложения");
            System.out.print("\n> ");
            String input = scanner.nextLine().trim();

            if (input.equalsIgnoreCase("3")) {
                log.info("👋 До свидания!");
                break;
            }

            processCommand(input, userId);
        }
    }

    private static int processUser() {
        log.info("👤 Введите пользователя от 1 до 10");
        Integer number = null;
        String input = scanner.nextLine().trim();
        try {
            number = Integer.parseInt(input);
            if (number >= 1 && number <= 10) {
                log.info("✅ Принято число: " + number);
            } else {
                log.error("❌ Число должно быть от 1 до 10. Попробуйте снова.");
            }
        } catch (NumberFormatException e) {
            log.error("❌ Допускается только ввод целых чисел");
        }
        return number;
    }

    private static void processCommand(String input, Integer userId) {
        switch (input) {
            case "1" -> processProduct(userId);
            case "2" -> getRecommendations(userId);
            default -> {log.info("Недоступная команда");}
        }
    }

    private static void processProduct(Integer userId) {
        log.info("Введите название товара:");
        String input = scanner.nextLine().trim();
        productService = new ProductService();
        List<String> products = productService.searchProducts(input);

        for (String product : products) {
            log.info("<> /n", product);
        }

        //TODO отправить запрос в кафка
    }

    private static void getRecommendations(Integer userId) {
        log.info("🎁 Получение рекомендаций для пользователя: {}", userId);

        try {
            // Получаем историю просмотров пользователя
            List<String> userHistory = getUserViewHistory(userId);
            recommendationService = new RecommendationService();
            // Получаем рекомендации на основе истории
            List<String> recommendations = recommendationService.generateRecommendations(userId, userHistory);

            displayRecommendations(recommendations, userId);

        } catch (Exception e) {
            log.error("❌ Ошибка при получении рекомендаций: {}", e.getMessage());
            // Fallback - показываем популярные товары
            log.info("Для пользователя {} еще нет рекомендаций", userId);
        }
    }

    private static List<String> getUserViewHistory(int userId) {
        // В реальном приложении здесь был бы запрос к базе данных
        // Сейчас возвращаем mock данные
        return List.of("electronics", "smartphones", "gadgets");
    }

    private static void displayRecommendations(List<String> recommendations, int userId) {
        log.info("✨ Персонализированные рекомендации для пользователя {}:", userId);

        for (String recomendation : recommendations){
            log.info("{}", recomendation);
        }
    }
}
