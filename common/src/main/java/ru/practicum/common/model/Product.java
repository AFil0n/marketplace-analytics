package ru.practicum.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
public class Product {
    @JsonProperty("product_id")
    private String productId;

    private String name;
    private String description;
    private Price price;
    private String category;
    private String brand;
    private Stock stock;
    private String sku;
    private List<String> tags;
    private List<Image> images;
    private Map<String, Object> specifications;

    @JsonProperty("created_at")
    private LocalDateTime createdAt;

    @JsonProperty("updated_at")
    private LocalDateTime updatedAt;

    private String index;

    @JsonProperty("store_id")
    private String storeId;

    @Data
    public static class Price {
        private Double amount;
        private String currency;
    }

    @Data
    public static class Stock {
        private Integer available;
        private Integer reserved;
    }

    @Data
    public static class Image {
        private String url;
        private String alt;
    }

    @Override
    public String toString(){
        return "Product{" +
                "id=" + productId +
                ", name='" + name + '\'' +
                ", price=" + price +
                ", category='" + category + '\'';
    }
}
