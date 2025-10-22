-- ksql/init.sql
-- Автоматическая инициализация ksqlDB при запуске

-- Ждем готовности топиков
SET 'auto.offset.reset' = 'earliest';

-- Создаем поток из топика пользовательских запросов
CREATE STREAM IF NOT EXISTS user_queries_stream (
    user_id VARCHAR KEY,
    search_query VARCHAR,
    category VARCHAR,
    timestamp VARCHAR,
    results_count INT
) WITH (
    KAFKA_TOPIC = 'userQuery',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
);

-- Создаем таблицу с последним запросом каждого пользователя
CREATE TABLE IF NOT EXISTS user_last_query WITH (
                                               KAFKA_TOPIC = 'user_last_query',
                                               VALUE_FORMAT = 'JSON',
                                               PARTITIONS = 3
                                               ) AS
SELECT
    user_id,
    LATEST_BY_OFFSET(search_query) AS last_search_query,
    LATEST_BY_OFFSET(category) AS last_category,
    LATEST_BY_OFFSET(timestamp) AS last_query_time
FROM user_queries_stream
GROUP BY user_id
    EMIT CHANGES;

-- Создаем поток рекомендаций
CREATE STREAM IF NOT EXISTS user_recommendations WITH (
    KAFKA_TOPIC = 'user_recommendations',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
) AS
SELECT
    user_id,
    last_search_query AS recommendation_text,
    'last_query' AS recommendation_type,
    last_category,
    last_query_time,
    CONCAT('Ваш последний поиск: "', last_search_query, '"') AS recommendation_message
FROM user_last_query
         EMIT CHANGES;

-- Показываем что создалось
SHOW STREAMS;
SHOW TABLES;