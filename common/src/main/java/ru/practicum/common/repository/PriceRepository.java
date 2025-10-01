package ru.practicum.common.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.practicum.common.model.Price;

import java.util.List;
import java.util.Optional;

@Repository
public interface PriceRepository extends JpaRepository<Price, Long> {
    Optional<Price> findByProductProductId(String productId);
    List<Price> findByCurrency(String currency);

    @Query("SELECT AVG(p.amount) FROM Price p WHERE p.currency = :currency")
    Double findAveragePriceByCurrency(@Param("currency") String currency);
}
