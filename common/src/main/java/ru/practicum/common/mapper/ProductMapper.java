package ru.practicum.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import ru.practicum.common.dto.ProductDTO;
import ru.practicum.common.model.Product;

@Mapper(componentModel = "spring")
public interface ProductMapper {

    ProductMapper INSTANCE = Mappers.getMapper(ProductMapper.class);

    @Mapping(target = "price.amount", source = "price.amount")
    @Mapping(target = "price.currency", source = "price.currency")
    @Mapping(target = "stock.available", source = "stock.available")
    @Mapping(target = "stock.reserved", source = "stock.reserved")
    Product toEntity(ProductDTO dto);

    @Mapping(target = "price.amount", source = "price.amount")
    @Mapping(target = "price.currency", source = "price.currency")
    @Mapping(target = "stock.available", source = "stock.available")
    @Mapping(target = "stock.reserved", source = "stock.reserved")
    ProductDTO toDto(Product entity);
}