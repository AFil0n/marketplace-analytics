package ru.practicum.client.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserQueryMessage {
    private Long userId;
    private String searchQuery;
    private Integer resultsCount;
}
