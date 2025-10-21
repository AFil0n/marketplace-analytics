package ru.practicum.common.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import ru.practicum.common.dto.ProductDTO;
import ru.practicum.common.model.Product;

@Mapper(componentModel = "spring")
public interface ProductMapper {

    ProductMapper INSTANCE = Mappers.getMapper(ProductMapper.class);

    @Mapping(target = "specifications", source = "specifications")
    @Mapping(target = "tags", source = "tags")
    @Mapping(target = "images", source = "images")
    @Mapping(target = "price", source = "price")
    @Mapping(target = "stock", source = "stock")
    @Mapping(target = "createdAt", source = "createdAt")
    @Mapping(target = "updatedAt", source = "updatedAt")
    Product toEntity(ProductDTO dto);

    @Mapping(target = "specifications", source = "specifications")
    @Mapping(target = "tags", source = "tags")
    @Mapping(target = "images", source = "images")
    @Mapping(target = "price", source = "price")
    @Mapping(target = "stock", source = "stock")
    @Mapping(target = "createdAt", source = "createdAt")
    @Mapping(target = "updatedAt", source = "updatedAt")
    ProductDTO toDto(Product entity);
}