package ru.practicum.common.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.practicum.common.repository.ProductRepository;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Service
public class ProductService {
    private final ProductRepository productRepository;

    public List<String> searchProducts(String productName){
        log.info("🔍 Поиск товаров по имени: {}", productName);
        return productRepository.searchProducts(productName);
    }
}
