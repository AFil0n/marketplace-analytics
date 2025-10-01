package ru.practicum.common.services;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ProductService {
    public List<String> searchProducts(String productName){
        log.info("🔍 Поиск товаров по имени: {}", productName);
        //TODO Написать получение продуктов из БД
        List<String> products = new ArrayList<>();
        products.add("test");

        return products;
    }
}
