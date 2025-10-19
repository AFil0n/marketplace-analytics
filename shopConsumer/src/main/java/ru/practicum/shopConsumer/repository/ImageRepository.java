package ru.practicum.shopConsumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.practicum.common.model.Image;
import java.util.List;

public interface ImageRepository extends JpaRepository<Image, Long> {

    List<Image> findByProductProductId(String productId);

    List<Image> findByProductProductIdOrderByOrderAsc(String productId);
}
