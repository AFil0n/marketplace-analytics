package ru.practicum.client.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.client.service.UserQueryService;

import java.util.List;

@RestController
@RequestMapping("/api/client")
@Slf4j
@RequiredArgsConstructor
public class ClientController {
    private final UserQueryService userQueryService;

    /**
     * Поиск товаров по имени (GET)
     */
    @GetMapping("/{userId}/search")
    public List<String> search(
            @RequestParam String query,
            @RequestParam(required = false) String userId) {
        return userQueryService.findProduct(userId, query);
    }

    /**
     * Получить рекомендацию для пользователя
     */
    @GetMapping("/{userId}/recommendation")
    public String getUserRecommendation(
            @PathVariable String userId) {
        return userQueryService.getUserRecomendation(userId);
    }
}
