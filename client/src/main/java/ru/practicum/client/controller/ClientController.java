package ru.practicum.client.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.client.service.RecommendationConsumer;
import ru.practicum.client.service.UserQueryService;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/client")
@Slf4j
@RequiredArgsConstructor
public class ClientController {
    private final UserQueryService userQueryService;
    private final RecommendationConsumer recommendationConsumer;

    /**
     * Получить рекомендацию для пользователя
     */
    @GetMapping("/{userId}/recommendation")
    public List<String> getUserRecommendation(
            @PathVariable Long userId) {
        RecommendationConsumer.UserRecommendation recommendation =
                recommendationConsumer.getRecommendation(userId);

        if (recommendation == null) {
            return List.of("error", "No recommendation found for user: " + userId);
        }

        return userQueryService.findProduct(userId, recommendation.getRecommendationMessage());
    }

    /**
     * Поиск товаров по имени (GET)
     */
    @GetMapping("/{userId}/search")
    public List<String> search(
            @PathVariable Long userId,
            @RequestParam String query
            ) {
        return userQueryService.findProduct(userId, query);
    }
}
