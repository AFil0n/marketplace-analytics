package ru.practicum.shopConsumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.common.model.Product;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

public interface ProductRepository extends JpaRepository<Product, String> {

    Optional<Product> findBySku(String sku);

    List<Product> findByCategory(String category);

    List<Product> findByBrand(String brand);

    List<Product> findByStoreStoreId(String storeId);

    @Query("SELECT p FROM Product p WHERE p.name LIKE %:name%")
    List<Product> findByNameContaining(@Param("name") String name);

    @Query("SELECT p FROM Product p JOIN p.tags t WHERE t = :tag")
    List<Product> findByTag(@Param("tag") String tag);

    List<Product> findByPriceAmountBetween(BigDecimal minPrice, BigDecimal maxPrice);
}
