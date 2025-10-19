package ru.practicum.shopConsumer.mapper;

import org.mapstruct.*;
import ru.practicum.common.model.Image;
import ru.practicum.common.model.Product;
import ru.practicum.shopConsumer.dto.ImageDTO;

import java.util.List;

@Mapper(componentModel = "spring")
public interface ImageMapper {

    @Mapping(target = "productId", source = "product.productId")
    ImageDTO toDTO(Image image);

    @Mapping(target = "product", source = "productId", qualifiedByName = "productIdToProduct")
    Image toEntity(ImageDTO imageDTO);

    List<ImageDTO> toDTOList(List<Image> images);
    List<Image> toEntityList(List<ImageDTO> imageDTOs);

    // Кастомные методы маппинга

    @Named("productIdToProduct")
    default Product productIdToProduct(String productId) {
        if (productId == null) {
            return null;
        }
        Product product = new Product();
        product.setProductId(productId);
        return product;
    }

    // Маппинг без циклических зависимостей
    @Mapping(target = "product", ignore = true)
    Image toEntityWithoutProduct(ImageDTO imageDTO);

    // Обновление entity из DTO
    @BeanMapping(nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
    void updateImageFromDTO(ImageDTO imageDTO, @MappingTarget Image image);

    // Маппинг для создания (игнорируем ID)
    @Mapping(target = "id", ignore = true)
    @Mapping(target = "product", source = "productId", qualifiedByName = "productIdToProduct")
    Image toNewEntity(ImageDTO imageDTO);
}