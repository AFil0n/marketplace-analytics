-- ksql/init.sql
SET 'auto.offset.reset' = 'earliest';

-- Удаляем старые объекты если есть
DROP TABLE IF EXISTS USER_RECOMMENDATIONS DELETE TOPIC;
DROP STREAM IF EXISTS USER_RECOMMENDATIONS_STREAM;

-- Создаем поток для camelCase формата
CREATE STREAM IF NOT EXISTS USER_QUERIES_RAW (
    userId BIGINT,
    searchQuery VARCHAR,
    resultsCount INT
) WITH (
    KAFKA_TOPIC = 'userQuery',
    VALUE_FORMAT = 'JSON'
);

-- Создаем таблицу рекомендаций СРАЗУ с правильным ключом
CREATE TABLE IF NOT EXISTS USER_RECOMMENDATIONS
    WITH (
        KAFKA_TOPIC = 'user_recommendations',
        VALUE_FORMAT = 'JSON'
        ) AS
SELECT
    CAST(userId AS STRING) AS USER_ID_STRING,
    LATEST_BY_OFFSET(searchQuery) AS RECOMMENDATION_TEXT,
    'last_query' AS RECOMMENDATION_TYPE,
    LATEST_BY_OFFSET(searchQuery) AS RECOMMENDATION_MESSAGE,
    LATEST_BY_OFFSET(resultsCount) AS LAST_RESULTS_COUNT
FROM USER_QUERIES_RAW
GROUP BY CAST(userId AS STRING)
    EMIT CHANGES;