package ru.practicum.common.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.practicum.common.model.Product;

import java.util.List;
import java.util.Optional;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {
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

    // Поиск по тегам (через JOIN)
    @Query("SELECT DISTINCT p FROM Product p JOIN p.tags t WHERE t.tag = :tag")
    List<Product> findByTag(@Param("tag") String tag);

    @Query("SELECT DISTINCT p FROM Product p JOIN p.tags t WHERE t.tag IN :tags")
    List<Product> findByTags(@Param("tags") List<String> tags);

    // Поиск по цене
    @Query("SELECT p FROM Product p JOIN p.price pr WHERE pr.amount BETWEEN :minPrice AND :maxPrice")
    List<Product> findByPriceRange(@Param("minPrice") Double minPrice, @Param("maxPrice") Double maxPrice);

    @Query("SELECT p FROM Product p JOIN p.price pr WHERE pr.currency = :currency")
    List<Product> findByCurrency(@Param("currency") String currency);

    // Поиск по наличию на складе
    @Query("SELECT p FROM Product p JOIN p.stock s WHERE s.available > 0")
    List<Product> findAvailableProducts();

    @Query("SELECT p FROM Product p JOIN p.stock s WHERE s.available > :minStock")
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
}
