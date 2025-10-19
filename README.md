Kafka Cluster with Disaster Recovery
Этот проект разворачивает отказоустойчивый Apache Kafka кластер с поддержкой Disaster Recovery (DR) используя Docker Compose.

# Архитектура
Primary Cluster (Локальный)
Контроллер: kafka-controller-0 (nodeId: 4000)

Брокеры:

kafka-0 (nodeId: 1000) - порты 1090, 1091

kafka-1 (nodeId: 2000) - порты 2090, 2091

UI: Kafka UI на порту 8090

Schema Registry: на портах 8081, 8082

DR Cluster (Disaster Recovery)
Контроллер: kafka-controller-dr-0 (nodeId: 5000)

Брокеры:

kafka-dr-0 (nodeId: 3000) - порты 3090, 3091

kafka-dr-1 (nodeId: 3100) - порты 3190, 3191

UI: Kafka UI на порту 8091

Schema Registry: на портах 6081, 6082

Репликация
MirrorMaker2: Реплицирует данные между primary и DR кластерами
 
# Запуск кластера
Предварительные требования
Docker

Docker Compose

Генерация SSL сертификатов (см. раздел Security)

Запуск всех сервисов
```bash
docker-compose up -d
```
Запуск отдельных компонентов
```bash
# Только primary кластер
docker-compose up -d kafka-controller-0 kafka-0 kafka-1 kafka-ui

# Только DR кластер
docker-compose up -d kafka-controller-dr-0 kafka-dr-0 kafka-dr-1 kafka-ui-dr

# Schema Registry
docker-compose up -d schema-registry schema-registry-dr
```
# MirrorMaker
docker-compose up -d mirror-maker
🔧 Конфигурация
Security Configuration
Authentication: SASL/PLAIN

Encryption: SSL/TLS

Authorization: StandardAuthorizer с ACL

Super User: admin/admin

Основные настройки Kafka
Primary Cluster
yaml
CLUSTER_ID: 'ciWo7IWazngRchmPES6q5A=='
KAFKA_PROCESS_ROLES: controller/broker
KAFKA_CONTROLLER_QUORUM_VOTERS: 4000@kafka-controller-0:4090
KAFKA_AUTO_CREATE_TOPICS_ENABLE: false
DR Cluster
yaml
CLUSTER_ID: 'drWo7IWazngRchmPES6q5B=='
KAFKA_PROCESS_ROLES: controller/broker
KAFKA_CONTROLLER_QUORUM_VOTERS: 5000@kafka-controller-dr-0:5090
KAFKA_AUTO_CREATE_TOPICS_ENABLE: true
Сетевые порты
Сервис	Порт	Назначение
kafka-0	1090	External listener
kafka-1	2090	External listener
kafka-dr-0	3090	External listener
kafka-dr-1	3190	External listener
kafka-ui	8090	Web Interface
kafka-ui-dr	8091	Web Interface
schema-registry	8081/8082	HTTP/HTTPS
schema-registry-dr	6081/6082	HTTP/HTTPS

# Мониторинг
Kafka UI
Primary: http://localhost:8090

DR: http://localhost:8091

Доступ с credentials: admin/admin

Health Checks
```bash
# Проверка состояния брокеров
docker-compose ps

# Логи конкретного сервиса
docker-compose logs kafka-0
docker-compose logs schema-registry
```

# Security
SSL Certificates
Требуется подготовить SSL сертификаты в директории ./cert/:

text
cert/
├── controller-creds/
│   ├── kafka.keystore.pkcs12
│   ├── kafka.truststore.jks
│   └── kafka_server_jaas.conf
└── kafka-0-creds/
├── kafka.keystore.pkcs12  
├── kafka.truststore.jks
└── kafka_server_jaas.conf
JAAS Configuration
Пример kafka_server_jaas.conf:

text
KafkaServer {
org.apache.kafka.common.security.plain.PlainLoginModule required
username="admin"
password="admin"
user_admin="admin"
user_producer="password"
user_consumer="password";
};

# MirrorMaker Configuration
MirrorMaker настраивается через конфигурационные файлы:

./infra/mirrormaker/consumer.cfg - конфигурация consumer для source кластера

./infra/mirrormaker/producer.cfg - конфигурация producer для target кластера

./infra/mirrormaker/start.sh - скрипт запуска

# Управление топиками
Инициализация топиков
```bash
docker-compose up kafka-init
```
Ручное создание топиков
```bash

# Подключение к брокеру
docker exec -it kafka-0 kafka-topics \
--bootstrap-server kafka-0:1090 \
--command-config /etc/kafka/secrets/client.properties \
--create --topic test-topic \
--partitions 3 --replication-factor 2
```
# Утилиты
Подключение через kafka-console-producer
```bash
docker exec -it kafka-0 kafka-console-producer \
--bootstrap-server kafka-0:1090 \
--topic test-topic \
--producer.config /etc/kafka/secrets/client.properties
```
Подключение через kafka-console-consumer
```bash
docker exec -it kafka-0 kafka-console-consumer \
--bootstrap-server kafka-0:1090 \
--topic test-topic \
--from-beginning \
--consumer.config /etc/kafka/secrets/client.properties
```
# Остановка и очистка
Остановка всех сервисов
```bash
docker-compose down
```
Остановка с удалением volumes
```bash
docker-compose down -v
```
Перезапуск конкретного сервиса
```bash
docker-compose restart kafka-0
```


# Модули

## shopProducer

ShopProducerApplication - это Java-приложение для пакетной обработки JSON-файлов с товарами и их публикации в Apache Kafka топик. Приложение автоматически регистрирует схему данных в Schema Registry и обрабатывает файлы из указанной директории.

## Архитектура
Основные компоненты:
Kafka Producer - отправка сообщений в Kafka

Schema Registry Client - регистрация и валидация схем данных

File Watcher - мониторинг директории на наличие новых файлов

JSON Parser - парсинг товаров из JSON файлов

## Структура директорий
text
/etc/
├── data/          # Входная директория для новых JSON файлов
├── ready/         # Директория для обработанных файлов  
└── schema/
└── product.json  # JSON схема для валидации товаров

## Конфигурация
Параметры приложения:
```java
private static final String INPUT_DIR = "/etc/data";
private static final String PROCESSED_DIR = "/etc/ready";
private static final String SCHEMA_PATH = "/etc/schema/product.json";
private static final long POLL_INTERVAL_MS = 5000L;
private static final long MESSAGE_DELAY_MS = 1000L;
```

Параметры Kafka (через KafkaProperties):
KafkaProperties.getProducerProperties() - настройки продюсера

KafkaProperties.getSchemaRegistryUrl() - URL Schema Registry

KafkaProperties.getShopProducerTopicName() - название топика

## Рабочий процесс
1. Инициализация

![img.png](img.png)


2. Обработка файлов

![img_1.png](img_1.png)


## Методы
main(String[] args)
Назначение: Точка входа приложения

Поток: Инициализация продюсера → регистрация схемы → запуск обработки файлов

registerSchema()
Назначение: Регистрация JSON схемы в Schema Registry

Использует: CachedSchemaRegistryClient

Схема: Читается из файла /etc/schema/product.json

loadSchemaFromFile()
Назначение: Загрузка схемы из файловой системы

Возвращает: JSON схему как строку

getFileProducts(String path)
Назначение: Парсинг списка товаров из JSON файла

Использует: Jackson ObjectMapper

Возвращает: List<Product> или null при ошибке

publishingProducts(Producer<String, Product> producer)
Назначение: Основной цикл обработки файлов

Логика:

Поиск JSON файлов в входной директории

Парсинг товаров из файла

Последовательная публикация каждого товара в Kafka

Перемещение обработанного файла

Пауза между проверками новых файлов

## Логирование
Приложение использует SLF4J для логирования ключевых событий:

✅Будет создано {}: - количество товаров для публикации

Публикуем в {}: {} - успешная публикация сообщения

Ошибка при публикации сообщения - ошибки при отправке в Kafka

Не удалось получить из файла: {} - проблемы с чтением файла


🚀 Запуск
Требования:
Apache Kafka кластер с включенным Schema Registry

SSL сертификаты (если используется SSL)

SASL аутентификация (если настроена)

Команда запуска:
```bash
docker-compose ud -d shopproducer
```

## Обработка ошибок
Ошибки парсинга JSON: Файл пропускается, ошибка логируется

Ошибки подключения к Kafka: Retry логика через Kafka Producer

Ошибки Schema Registry: Приложение останавливается с исключением

Проблемы с файловой системой: Логируются, продолжается работа
 
## Производительность
Задержка между сообщениями: 1000 мс

Интервал проверки файлов: 5000 мс

Размер пула Schema Registry: 10 соединений

Обработка в одном потоке

## Безопасность
Приложение использует те же настройки безопасности, что и основной Kafka кластер:

SASL/PLAIN аутентификация

SSL/TLS шифрование

Настройки через KafkaProperties

## Управление состоянием
Статус файлов: Отслеживается через перемещение между директориями

Схемы: Кэшируются Schema Registry клиентом

Подключения: Управляются через try-with-resources

Эта документация покрывает все аспекты работы Shop Producer Application для интеграции с вашим Kafka кластером.