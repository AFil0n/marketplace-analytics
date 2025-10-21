package ru.practicum.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Data
public class ProductDTO {

    @JsonProperty("product_id")
    private String productId;

    private String name;
    private String description;
    private String category;
    private String brand;
    private String sku;

    @JsonProperty("store_id")
    private String storeId;

    @JsonProperty("created_at")
    private String createdAt;

    @JsonProperty("updated_at")
    private String updatedAt;

    private String index;

    private List<String> tags;
    private List<ImageDTO> images;
    private Map<String, String> specifications;

    private PriceDTO price;
    private StockDTO stock;

    @Data
    public static class PriceDTO {
        private BigDecimal amount;
        private String currency;
    }

    @Data
    public static class StockDTO {
        private Integer available;
        private Integer reserved;
    }

    @Data
    public static class ImageDTO {
        private String url;
        private String alt;
    }
}
