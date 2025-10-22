package ru.practicum.client.dto;

import lombok.Data;
import ru.practicum.client.model.UserQuery;

@Data
public class UserQueryDTO {
    private String userId;
    private String searchQuery;
    private String category;
    private Integer resultsCount;

    public UserQueryDTO(UserQuery userQuery) {
        this.userId = userQuery.getUserId();
        this.searchQuery = userQuery.getSearchQuery();
        this.category = userQuery.getCategory();
    }

    public UserQueryDTO(String userId, String searchQuery, String category) {
        this.userId = userId;
        this.searchQuery = searchQuery;
        this.category = category;
    }
}
