package ru.practicum.client.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.client.dto.UserQueryDTO;
import ru.practicum.client.model.UserQuery;
import ru.practicum.client.repository.UserQueryRepository;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserQueryService {
    private final UserQueryRepository userQueryRepository;
    private final ProductService productService;
    private final KafkaTemplate<String, Object> kafkaTemplate; // Изменен тип
    private final ObjectMapper objectMapper;

    public void saveAndPublishUserQuery(Long userId, String searchQuery) {
        log.info("✅ Start work saveAndPublishUserQuery user={}, query={}", userId, searchQuery);
        try {
            UserQuery userQuery = new UserQuery();
            userQuery.setUserId(userId);
            userQuery.setSearchQuery(searchQuery);

            var savedQuery = saveUserQuery(userQuery);
            UserQueryDTO userQueryDTO = new UserQueryDTO(savedQuery);

            // Отправляем DTO объект напрямую, JsonSerializer сериализует его
            kafkaTemplate.send("userQuery", userId.toString(), userQueryDTO)
                    .whenComplete((result, exception) -> {
                        if (exception == null) {
                            log.info("✅ User query published to Kafka: user={}, offset={}",
                                    userId, result.getRecordMetadata().offset());
                        } else {
                            log.error("❌ Failed to publish user query to Kafka: user={}", userId, exception);
                        }
                    });

        } catch (Exception e) {
            log.error("❌ Error processing user query: user={}, query={}", userId, searchQuery, e);
        }
    }

    @Transactional
    public UserQuery saveUserQuery(UserQuery userQuery){
        UserQuery savedQuery = userQueryRepository.saveAndFlush(userQuery);

        log.info("✅ User query saved to DB: id={}, user={}, query={}",
                savedQuery.getId(), userQuery.getUserId(), userQuery.getSearchQuery());

        return savedQuery;
    }

    public List<String> findProduct(Long userId, String query) {
        saveAndPublishUserQuery(userId, query);
        return productService.searchProducts(query);
    }
}