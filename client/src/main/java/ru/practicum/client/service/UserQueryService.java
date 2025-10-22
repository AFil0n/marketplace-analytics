package ru.practicum.client.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.practicum.client.dto.UserQueryDTO;
import ru.practicum.client.model.UserQuery;
import ru.practicum.client.repository.UserQueryRepository;
import ru.practicum.common.services.ProductService;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserQueryService {
    private final UserQueryRepository userQueryRepository;
    private final ProductService productService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;


    public void saveAndPublishUserQuery(String userId, String searchQuery) {
        try {
            UserQuery userQuery = new UserQuery();
            userQuery.setUserId(userId);
            userQuery.setSearchQuery(searchQuery);

            UserQuery savedQuery = userQueryRepository.save(userQuery);

            log.info("✅ User query saved to DB: id={}, user={}, query={}, results={}",
                    savedQuery.getId(), userId, searchQuery);

            // Отправляем в Kafka
            UserQueryDTO userQueryDTO = new UserQueryDTO(savedQuery);
            String userQueryJson = objectMapper.writeValueAsString(userQueryDTO);

            kafkaTemplate.send("userQuery", userId, userQueryJson)
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

    public String getUserRecomendation(String userId) {
        return "";
    }

    public List<String> findProduct(String userId, String query) {
        saveAndPublishUserQuery(userId, query);
        return productService.searchProducts(query);
    }
}
