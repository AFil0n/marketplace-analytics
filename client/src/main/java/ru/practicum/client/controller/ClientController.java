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
    public ResponseEntity<Map<String, Object>> getUserRecommendation(
            @PathVariable String userId) {
        RecommendationConsumer.UserRecommendation recommendation =
                recommendationConsumer.getRecommendation(userId);

        if (recommendation == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "No recommendation found for user: " + userId));
        }

        return ResponseEntity.ok(Map.of(
                "user_id", userId,
                "recommendation", recommendation.getRecommendation_message(),
                "type", recommendation.getRecommendation_type(),
                "last_query_time", recommendation.getLast_query_time()
        ));
    }

    /**
     * Поиск товаров по имени (GET)
     */
    @GetMapping("/{userId}/search")
    public List<String> search(
            @RequestParam String query,
            @RequestParam(required = false) String userId) {
        return userQueryService.findProduct(userId, query);
    }
}
