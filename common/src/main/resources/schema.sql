-- Создание схемы если не существует
CREATE SCHEMA IF NOT EXISTS shop;

-- Таблица магазинов
CREATE TABLE IF NOT EXISTS shop.stores (
    store_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Основная таблица продуктов
CREATE TABLE IF NOT EXISTS shop.products (
    product_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    description TEXT,
    category VARCHAR(200) NOT NULL,
    brand VARCHAR(200),
    sku VARCHAR(100) UNIQUE,
    index VARCHAR(100),
    store_id VARCHAR(50),

    -- Встроенные объекты
    price_amount DECIMAL(12,2),
    price_currency VARCHAR(3),
    stock_available INTEGER DEFAULT 0,
    stock_reserved INTEGER DEFAULT 0,

    -- Даты
    created_at TIMESTAMP,
    updated_at TIMESTAMP,

    -- Индексы
    CONSTRAINT fk_product_store
    FOREIGN KEY (store_id)
    REFERENCES shop.stores(store_id)
    ON DELETE SET NULL
    );

-- Таблица тегов продуктов
CREATE TABLE IF NOT EXISTS shop.product_tags (
                                                 id BIGSERIAL PRIMARY KEY,
                                                 product_id VARCHAR(50) NOT NULL,
    tag VARCHAR(200) NOT NULL,

    CONSTRAINT fk_tag_product
    FOREIGN KEY (product_id)
    REFERENCES shop.products(product_id)
    ON DELETE CASCADE,

    CONSTRAINT unique_product_tag
    UNIQUE (product_id, tag)
    );

-- Таблица изображений продуктов
CREATE TABLE IF NOT EXISTS shop.product_images (
                                                   id BIGSERIAL PRIMARY KEY,
                                                   product_id VARCHAR(50) NOT NULL,
    url TEXT NOT NULL,
    alt TEXT,
    sort_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_image_product
    FOREIGN KEY (product_id)
    REFERENCES shop.products(product_id)
    ON DELETE CASCADE
    );

-- Таблица характеристик продуктов
CREATE TABLE IF NOT EXISTS shop.product_specifications (
                                                           id BIGSERIAL PRIMARY KEY,
                                                           product_id VARCHAR(50) NOT NULL,
    spec_key VARCHAR(200) NOT NULL,
    spec_value TEXT,

    CONSTRAINT fk_spec_product
    FOREIGN KEY (product_id)
    REFERENCES shop.products(product_id)
    ON DELETE CASCADE,

    CONSTRAINT unique_product_spec
    UNIQUE (product_id, spec_key)
    );