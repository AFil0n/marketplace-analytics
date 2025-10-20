-- Индексы для таблицы продуктов
CREATE INDEX IF NOT EXISTS idx_products_category ON shop.products(category);
CREATE INDEX IF NOT EXISTS idx_products_brand ON shop.products(brand);
CREATE INDEX IF NOT EXISTS idx_products_store ON shop.products(store_id);
CREATE INDEX IF NOT EXISTS idx_products_sku ON shop.products(sku);
CREATE INDEX IF NOT EXISTS idx_products_created_at ON shop.products(created_at);
CREATE INDEX IF NOT EXISTS idx_products_price ON shop.products(price_amount);
CREATE INDEX IF NOT EXISTS idx_products_stock ON shop.products(stock_available);

-- Индексы для таблицы тегов
CREATE INDEX IF NOT EXISTS idx_product_tags_tag ON shop.product_tags(tag);
CREATE INDEX IF NOT EXISTS idx_product_tags_product ON shop.product_tags(product_id);

-- Индексы для таблицы изображений
CREATE INDEX IF NOT EXISTS idx_product_images_product ON shop.product_images(product_id);
CREATE INDEX IF NOT EXISTS idx_product_images_order ON shop.product_images(sort_order);
CREATE INDEX IF NOT EXISTS idx_product_images_url ON shop.product_images(url);

-- Индексы для таблицы характеристик
CREATE INDEX IF NOT EXISTS idx_product_specs_product ON shop.product_specifications(product_id);
CREATE INDEX IF NOT EXISTS idx_product_specs_key ON shop.product_specifications(spec_key);