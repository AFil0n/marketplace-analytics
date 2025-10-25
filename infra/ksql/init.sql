-- ksql/init.sql
-- Автоматическая инициализация ksqlDB при запуске

SET 'auto.offset.reset' = 'earliest';

-- Создаем поток для camelCase формата (как отправляет Spring)
CREATE STREAM IF NOT EXISTS USER_QUERIES_RAW (
    userId BIGINT,
    searchQuery VARCHAR,
    resultsCount INT
) WITH (
    KAFKA_TOPIC = 'userQuery',
    VALUE_FORMAT = 'JSON'
);

-- Преобразуем в UPPERCASE для consistency
CREATE STREAM IF NOT EXISTS USER_QUERIES_CLEAN AS
SELECT
    userId AS USERID,
    searchQuery AS SEARCHQUERY,
    resultsCount AS RESULTSCOUNT
FROM USER_QUERIES_RAW
         EMIT CHANGES;

-- Остальное без изменений
CREATE TABLE IF NOT EXISTS USER_LAST_QUERY
    WITH (
        KAFKA_TOPIC = 'user_last_query',
        VALUE_FORMAT = 'JSON'
        ) AS
SELECT
    USERID AS USER_ID,
    LATEST_BY_OFFSET(SEARCHQUERY) AS LAST_SEARCH_QUERY,
    LATEST_BY_OFFSET(RESULTSCOUNT) AS LAST_RESULTS_COUNT
FROM USER_QUERIES_CLEAN
GROUP BY USERID
    EMIT CHANGES;

CREATE TABLE IF NOT EXISTS USER_RECOMMENDATIONS
    WITH (
        KAFKA_TOPIC = 'user_recommendations',
        VALUE_FORMAT = 'JSON'
        ) AS
SELECT
    USER_ID,
    LAST_SEARCH_QUERY AS RECOMMENDATION_TEXT,
    'last_query' AS RECOMMENDATION_TYPE,
    CONCAT('Ваш последний поиск: "', LAST_SEARCH_QUERY, '"') AS RECOMMENDATION_MESSAGE,
    LAST_RESULTS_COUNT
FROM USER_LAST_QUERY
         EMIT CHANGES;