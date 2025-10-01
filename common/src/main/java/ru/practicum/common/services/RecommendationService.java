package ru.practicum.common.services;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class RecommendationService {

    private final Map<String, List<String>> userPreferences;
    private final Map<String, List<String>> categoryRelations;

    public RecommendationService() {
        this.userPreferences = new HashMap<>();
        this.categoryRelations = initializeCategoryRelations();
        initializeSampleData();
    }

    private Map<String, List<String>> initializeCategoryRelations() {
        Map<String, List<String>> relations = new HashMap<>();

        relations.put("electronics", Arrays.asList("smartphones", "laptops", "tablets", "headphones"));
        relations.put("smartphones", Arrays.asList("phone_cases", "screen_protectors", "chargers"));
        relations.put("laptops", Arrays.asList("laptop_bags", "mouse", "keyboards"));
        relations.put("clothing", Arrays.asList("shoes", "accessories", "jackets"));
        relations.put("books", Arrays.asList("bookmarks", "reading_lights", "book_covers"));

        return relations;
    }

    private void initializeSampleData() {
        // Пример предпочтений пользователей
        userPreferences.put("user1", Arrays.asList("electronics", "gadgets", "smartphones"));
        userPreferences.put("user2", Arrays.asList("books", "stationery", "art_supplies"));
        userPreferences.put("user3", Arrays.asList("clothing", "shoes", "accessories"));
        userPreferences.put("user4", Arrays.asList("home", "kitchen", "furniture"));
    }

    public List<String> generateRecommendations(int userId, List<String> userHistory) {
        log.debug("Генерация рекомендаций для пользователя: {}, история: {}", userId, userHistory);

        Set<String> recommendations = new LinkedHashSet<>();

        // 1. Рекомендации на основе истории просмотров
        for (String category : userHistory) {
            recommendations.addAll(getRelatedCategories(category));
        }

        // 2. Рекомендации на основе предпочтений пользователя
        if (userPreferences.containsKey(userId)) {
            recommendations.addAll(userPreferences.get(userId));
        }

        // 3. Популярные категории (fallback)
        if (recommendations.isEmpty()) {
            recommendations.addAll(Arrays.asList("electronics", "clothing", "books", "home"));
        }

        return new ArrayList<>(recommendations).subList(0, Math.min(recommendations.size(), 5));
    }

    private List<String> getRelatedCategories(String category) {
        return categoryRelations.getOrDefault(category, Arrays.asList(category));
    }

    public void updateUserPreferences(String userId, String category) {
        userPreferences.computeIfAbsent(userId, k -> new ArrayList<>()).add(category);
        log.info("✅ Обновлены предпочтения пользователя {}: добавлена категория {}", userId, category);
    }
}
