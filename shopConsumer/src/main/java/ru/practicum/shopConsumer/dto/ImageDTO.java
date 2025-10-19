package ru.practicum.shopConsumer.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ImageDTO {
    private Long id;
    private String url;
    private String alt;
    private Integer order;
    private String productId;
}