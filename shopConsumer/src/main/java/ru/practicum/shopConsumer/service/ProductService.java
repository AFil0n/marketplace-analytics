package ru.practicum.shopConsumer.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.practicum.common.model.Product;
import ru.practicum.common.repository.ProductRepository;
import ru.practicum.shopConsumer.dto.ProductDTO;
import ru.practicum.shopConsumer.mapper.ProductMapper;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;
    private final ProductMapper productMapper;

    public ProductDTO getProductById(Long productId) {
        return productRepository.findById(productId)
                .map(productMapper::toDTO)
                .orElse(null);
    }

    public List<ProductDTO> getAllProducts() {
        return productRepository.findAll().stream()
                .map(productMapper::toDTO)
                .collect(Collectors.toList());
    }

    public ProductDTO createProduct(ProductDTO productDTO) {
        Product product = productMapper.toEntity(productDTO);
        Product saved = productRepository.save(product);
        return productMapper.toDTO(saved);
    }

    public List<ProductDTO> getProductsByCategory(String category) {
        return productRepository.findByCategory(category).stream()
                .map(productMapper::toDTO)
                .collect(Collectors.toList());
    }
}
