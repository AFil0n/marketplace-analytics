package ru.practicum.client.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import ru.practicum.client.model.UserQuery;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserQueryDTO {
    private Long userId;
    private String searchQuery;
    private Integer resultsCount;

    public UserQueryDTO(UserQuery userQuery) {
        this.userId = userQuery.getUserId();
        this.searchQuery = userQuery.getSearchQuery();
    }

    public UserQueryDTO(Long userId, String searchQuery, String category) {
        this.userId = userId;
        this.searchQuery = searchQuery;
    }
}
