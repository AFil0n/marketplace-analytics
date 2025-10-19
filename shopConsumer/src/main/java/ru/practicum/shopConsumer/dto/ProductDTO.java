package ru.practicum.shopConsumer.dto;

import lombok.Data;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
public class ProductDTO {
    private String productId;
    private String name;
    private String description;
    private BigDecimal priceAmount;
    private String priceCurrency;
    private String category;
    private String brand;
    private Integer stockAvailable;
    private Integer stockReserved;
    private String sku;
    private List<String> tags;
    private List<ImageDTO> images;
    private Map<String, String> specifications;
    private String storeId;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}