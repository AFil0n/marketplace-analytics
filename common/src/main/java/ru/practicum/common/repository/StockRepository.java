package ru.practicum.common.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.common.model.Stock;

import java.util.Optional;

public interface StockRepository extends JpaRepository<Stock, Long> {
    Optional<Stock> findByProductProductId(String productId);

    @Modifying
    @Query("UPDATE Stock s SET s.available = s.available - :quantity WHERE s.product.productId = :productId AND s.available >= :quantity")
    int reserveStock(@Param("productId") String productId, @Param("quantity") Integer quantity);

    @Modifying
    @Query("UPDATE Stock s SET s.reserved = s.reserved - :quantity WHERE s.product.productId = :productId")
    int releaseReservedStock(@Param("productId") String productId, @Param("quantity") Integer quantity);
}
