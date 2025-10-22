package ru.practicum.common.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.practicum.common.model.Product;

import java.util.List;

@Repository
public interface ProductRepository extends JpaRepository<Product, String> {

    @Query("SELECT p.name FROM Product p WHERE p.name LIKE '%' || :productName || '%'")
    List<String> searchProducts(@Param("productName") String productName);
}