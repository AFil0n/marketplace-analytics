package ru.practicum.common.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.practicum.common.model.Product;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

@Repository
public interface ProductRepository extends JpaRepository<Product, String> {

    // Базовые методы
    Optional<Product> findByProductId(String productId);
    Optional<Product> findBySku(String sku);
    List<Product> findByStoreId(String storeId);
    boolean existsByProductId(String productId);
    boolean existsBySku(String sku);

    // Поисковые методы
    List<Product> findByNameContainingIgnoreCase(String name);
    List<Product> findByDescriptionContainingIgnoreCase(String keyword);
    List<Product> findByCategory(String category);
    List<Product> findByBrand(String brand);
    List<Product> findByCategoryAndBrand(String category, String brand);

    // Поиск по цене (исправлено для BigDecimal)
    @Query("SELECT p FROM Product p WHERE p.price.amount BETWEEN :minPrice AND :maxPrice")
    List<Product> findByPriceRange(@Param("minPrice") BigDecimal minPrice, @Param("maxPrice") BigDecimal maxPrice);

    @Query("SELECT p FROM Product p WHERE p.price.currency = :currency")
    List<Product> findByCurrency(@Param("currency") String currency);

    // Поиск по наличию на складе
    @Query("SELECT p FROM Product p WHERE p.stock.available > 0")
    List<Product> findAvailableProducts();

    @Query("SELECT p FROM Product p WHERE p.stock.available > :minStock")
    List<Product> findByMinStock(@Param("minStock") Integer minStock);

    // Сортировка
    List<Product> findAllByOrderByCreatedAtDesc();
    List<Product> findAllByOrderByNameAsc();

    // Пагинационные запросы
    @Query("SELECT p FROM Product p WHERE p.category = :category ORDER BY p.createdAt DESC")
    List<Product> findLatestByCategory(@Param("category") String category);

    // Статистические запросы
    @Query("SELECT COUNT(p) FROM Product p WHERE p.category = :category")
    Long countByCategory(@Param("category") String category);

    @Query("SELECT p.category, COUNT(p) FROM Product p GROUP BY p.category")
    List<Object[]> countProductsByCategory();

    // Удаление
    void deleteByProductId(String productId);
    void deleteByStoreId(String storeId);

    // Дополнительные методы с пагинацией
    Page<Product> findByCategory(String category, Pageable pageable);
    Page<Product> findByBrand(String brand, Pageable pageable);
    Page<Product> findByNameContainingIgnoreCase(String name, Pageable pageable);
}