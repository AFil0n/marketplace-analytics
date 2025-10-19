package ru.practicum.shopConsumer.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.practicum.shopConsumer.dto.ImageDTO;
import ru.practicum.shopConsumer.mapper.ImageMapper;
import ru.practicum.shopConsumer.repository.ImageRepository;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ImageService {

    private final ImageRepository imageRepository;
    private final ImageMapper imageMapper;

    public List<ImageDTO> getProductImages(String productId) {
        return imageRepository.findByProductProductIdOrderByOrderAsc(productId).stream()
                .map(imageMapper::toDTO)
                .collect(Collectors.toList());
    }

    public ImageDTO addImageToProduct(ImageDTO imageDTO) {
        var image = imageMapper.toEntity(imageDTO);
        var saved = imageRepository.save(image);
        return imageMapper.toDTO(saved);
    }
}
