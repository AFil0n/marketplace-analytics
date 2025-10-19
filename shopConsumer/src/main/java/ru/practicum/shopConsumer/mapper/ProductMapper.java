package ru.practicum.shopConsumer.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import ru.practicum.common.model.Product;
import ru.practicum.shopConsumer.dto.ProductDTO;

import java.util.List;

@Mapper(componentModel = "spring")
public interface ProductMapper {

    ProductMapper INSTANCE = Mappers.getMapper(ProductMapper.class);

    @Mapping(target = "priceAmount", source = "price.amount")
    @Mapping(target = "priceCurrency", source = "price.currency")
    @Mapping(target = "stockAvailable", source = "stock.available")
    @Mapping(target = "stockReserved", source = "stock.reserved")
    @Mapping(target = "storeId", source = "store.storeId")
    ProductDTO toDTO(Product product);

    @Mapping(target = "price.amount", source = "priceAmount")
    @Mapping(target = "price.currency", source = "priceCurrency")
    @Mapping(target = "stock.available", source = "stockAvailable")
    @Mapping(target = "stock.reserved", source = "stockReserved")
    @Mapping(target = "store.storeId", source = "storeId")
    Product toEntity(ProductDTO productDTO);

    List<ProductDTO> toDTOList(List<Product> products);
    List<Product> toEntityList(List<ProductDTO> productDTOs);
}
