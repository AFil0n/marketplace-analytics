CREATE TABLE IF NOT EXISTS products (
id SERIAL PRIMARY KEY,
product_id VARCHAR(255) NOT NULL UNIQUE,
name VARCHAR(255) NOT NULL,
description TEXT,
category VARCHAR(255) NOT NULL,
brand VARCHAR(255) NOT NULL,
sku VARCHAR(255) NOT NULL,
store_id VARCHAR(255) NOT NULL,
index VARCHAR(50) NOT NULL DEFAULT 'products',
created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

-- Индексы для оптимизации поиска
CONSTRAINT idx_products_product_id UNIQUE (product_id),
CONSTRAINT idx_products_sku UNIQUE (sku)
);

-- Create price table
CREATE TABLE IF NOT EXISTS product_prices (
id SERIAL PRIMARY KEY,
product_id VARCHAR(255) NOT NULL,
amount DECIMAL(15,2) NOT NULL CHECK (amount >= 0),
currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT fk_product_prices_product
FOREIGN KEY (product_id)
REFERENCES products(product_id)
ON DELETE CASCADE,

CONSTRAINT idx_product_prices_product_id UNIQUE (product_id)
);

-- Create stock table
CREATE TABLE IF NOT EXISTS product_stock (
id SERIAL PRIMARY KEY,
product_id VARCHAR(255) NOT NULL,
available INTEGER NOT NULL CHECK (available >= 0),
reserved INTEGER NOT NULL CHECK (reserved >= 0) DEFAULT 0,
created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT fk_product_stock_product
FOREIGN KEY (product_id)
REFERENCES products(product_id)
ON DELETE CASCADE,

CONSTRAINT idx_product_stock_product_id UNIQUE (product_id)
);

-- Create tags table (many-to-many relationship)
CREATE TABLE IF NOT EXISTS product_tags (
id SERIAL PRIMARY KEY,
product_id VARCHAR(255) NOT NULL,
tag VARCHAR(100) NOT NULL,
created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT fk_product_tags_product
FOREIGN KEY (product_id)
REFERENCES products(product_id)
ON DELETE CASCADE,

CONSTRAINT idx_product_tags_unique UNIQUE (product_id, tag)
);

-- Create images table
CREATE TABLE IF NOT EXISTS product_images (
id SERIAL PRIMARY KEY,
product_id VARCHAR(255) NOT NULL,
url TEXT NOT NULL,
alt_text VARCHAR(255),
sort_order INTEGER DEFAULT 0,
created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT fk_product_images_product
FOREIGN KEY (product_id)
REFERENCES products(product_id)
ON DELETE CASCADE
);

-- Create specifications table (flexible key-value storage)
CREATE TABLE IF NOT EXISTS product_specifications (
id SERIAL PRIMARY KEY,
product_id VARCHAR(255) NOT NULL,
spec_key VARCHAR(100) NOT NULL,
spec_value TEXT,
created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

CONSTRAINT fk_product_specifications_product
FOREIGN KEY (product_id)
REFERENCES products(product_id)
ON DELETE CASCADE,

CONSTRAINT idx_product_specs_unique UNIQUE (product_id, spec_key)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
CREATE INDEX IF NOT EXISTS idx_products_store_id ON products(store_id);
CREATE INDEX IF NOT EXISTS idx_products_created_at ON products(created_at);
CREATE INDEX IF NOT EXISTS idx_product_tags_tag ON product_tags(tag);
CREATE INDEX IF NOT EXISTS idx_product_prices_currency ON product_prices(currency);
CREATE INDEX IF NOT EXISTS idx_product_stock_available ON product_stock(available);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_product_prices_updated_at
    BEFORE UPDATE ON product_prices
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_product_stock_updated_at
    BEFORE UPDATE ON product_stock
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();