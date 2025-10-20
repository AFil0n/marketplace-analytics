package ru.practicum.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.javafaker.Faker;
import ru.practicum.common.dto.ImageDTO;
import ru.practicum.common.dto.PriceDTO;
import ru.practicum.common.dto.ProductDTO;
import ru.practicum.common.dto.StockDTO;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ProductGenerator {
    private static final Faker faker = new Faker(new Locale("ru"));
    private static final ObjectMapper objectMapper = createObjectMapper();
    private static final String DATA_DIR = "data";

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    private static final List<String> CATEGORIES = Arrays.asList(
            "Электроника", "Одежда", "Обувь", "Аксессуары", "Книги",
            "Спорт", "Красота", "Дом", "Садоводство", "Игрушки"
    );

    private static final List<String> CURRENCIES = Arrays.asList("RUB", "USD", "EUR", "GBP");
    private static final List<String> STORE_IDS = Arrays.asList("store_001", "store_002", "store_003", "store_004");

    public static List<ProductDTO> generateProducts(int count) {
        List<ProductDTO> products = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            ProductDTO product = new ProductDTO();

//            product.setProduct_id(faker.number().digits(5));
//            product.setName(faker.commerce().productName());
//            product.setDescription(faker.lorem().sentence(10));
//            product.setPrice(generatePrice());
//            product.setCategory(faker.options().nextElement(CATEGORIES));
//            product.setBrand(faker.company().name());
//            product.setStock(generateStock());
//            product.setSku(faker.regexify("[A-Z]{3}-[0-9]{5}"));
//            product.setTags(new ArrayList<>());
//            product.setImages(generateImages());
//            product.setSpecifications((List<ProductSpecificationDTO>) generateSpecifications());
//            product.setCreated_at(generatePastDate(30));
//            product.setUpdated_at(generatePastDate(10));
//            product.setIndex("products");
//            product.setStore_id(faker.options().nextElement(STORE_IDS));

            products.add(product);
        }

        return products;
    }

    private static PriceDTO generatePrice() {
        PriceDTO price = new PriceDTO();
        double amount = 500 + (50000 - 500) * faker.random().nextDouble();
        amount = Math.round(amount * 100.0) / 100.0; // Округляем до 2 знаков после запятой
        price.setAmount(BigDecimal.valueOf(amount));
        price.setCurrency(faker.options().nextElement(CURRENCIES));
        return price;
    }

    private static StockDTO generateStock() {
        StockDTO stock = new StockDTO();
        int available = faker.number().numberBetween(0, 500);
        stock.setAvailable(available);
        stock.setReserved(faker.number().numberBetween(0, available));
        return stock;
    }

    private static List<String> generateTags() {
        int count = faker.number().numberBetween(1, 5);
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            tags.add(faker.lorem().word());
        }
        return tags;
    }

    private static List<ImageDTO> generateImages() {
        int count = faker.number().numberBetween(1, 4);
        List<ImageDTO> images = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ImageDTO image = new ImageDTO();
            image.setUrl("https://example.com/images/" + faker.internet().slug() + ".jpg");
            image.setAlt(faker.lorem().sentence(3));
            images.add(image);
        }
        return images;
    }

    private static Map<String, String> generateSpecifications() {
        Map<String, String> specs = new HashMap<>();
        specs.put("weight", faker.number().numberBetween(10, 1000) + "g");
        specs.put("dimensions",
                faker.number().numberBetween(10, 100) + "mm x " +
                        faker.number().numberBetween(10, 100) + "mm x " +
                        faker.number().numberBetween(5, 50) + "mm");
        specs.put("battery_life", faker.number().numberBetween(1, 72) + " hours");
        specs.put("material", faker.commerce().material());
        return specs;
    }

    private static LocalDateTime generatePastDate(int maxDaysAgo) {
        int daysAgo = faker.number().numberBetween(1, maxDaysAgo);
        return LocalDateTime.now().minusDays(daysAgo);
    }

    private static void saveToFile(List<ProductDTO> products, String filename) {
        try {
            // Создаем папку data если ее нет
            Path dataDir = Paths.get(DATA_DIR);
            if (!Files.exists(dataDir)) {
                Files.createDirectories(dataDir);
                System.out.println("Создана папка: " + DATA_DIR);
            }

            // Сохраняем в файл
            Path filePath = dataDir.resolve(filename);
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(products);
            Files.write(filePath, json.getBytes());

            System.out.println("Файл сохранен: " + filePath.toAbsolutePath());
            System.out.println("Размер файла: " + Files.size(filePath) + " байт");

        } catch (IOException e) {
            System.err.println("Ошибка при сохранении файла: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        List<ProductDTO> products = generateProducts(100000);
//        try {
//            for (int i = 0; i < products.size(); i++) {
//                System.out.println("=== ВСЕ ПРОДУКТЫ В ФОРМАТЕ JSON ===");
//                String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(products);
//                System.out.println(json);
//
//                System.out.println("\n" + "=".repeat(50) + "\n");
//            }

            System.out.println("=== СОХРАНЕНИЕ В ФАЙЛ ===");
            saveToFile(products, "products.json");
//        } catch (JsonProcessingException e){
//            System.err.println("Ошибка при преобразовании в JSON: " + e.getMessage());
//        }


        System.out.println("Всего сгенерировано продуктов: " + products.size());
    }
}
