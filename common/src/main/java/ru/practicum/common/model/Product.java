package ru.practicum.common.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Entity
@Table(name = "products", schema = "shop")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Product {

    @Id
    @EqualsAndHashCode.Include
    private String productId;

    @Column(nullable = false)
    private String name;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Embedded
    private Price price;

    @Column(nullable = false)
    private String category;

    private String brand;

    @Embedded
    private Stock stock;

    @Column(unique = true)
    private String sku;

    @ElementCollection
    @CollectionTable(
            name = "product_tags",
            schema = "shop",
            joinColumns = @JoinColumn(name = "product_id")
    )
    @Column(name = "tag")
    private List<String> tags = new ArrayList<>();

    @OneToMany(mappedBy = "product", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private List<Image> images = new ArrayList<>();

    @ElementCollection
    @CollectionTable(
            name = "product_specifications",
            schema = "shop",
            joinColumns = @JoinColumn(name = "product_id")
    )
    @MapKeyColumn(name = "spec_key")
    @Column(name = "spec_value")
    private Map<String, String> specifications;

    private String index;

    @Column(name = "store_id")
    private String storeId;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
}