-- Создание схемы если не существует
CREATE SCHEMA IF NOT EXISTS client;

-- Таблица магазинов
CREATE TABLE IF NOT EXISTS user_queries (
      id BIGSERIAL PRIMARY KEY,
      user_id BIGINT NOT NULL,
      search_query VARCHAR(255) NOT NULL
);
