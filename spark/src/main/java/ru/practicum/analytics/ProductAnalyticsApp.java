package ru.practicum.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.Metadata;


public class ProductAnalyticsApp {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // Создаем Spark сессию
        SparkSession spark = SparkSession.builder()
                .appName("ProductAnalytics")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("🚀 Starting Product Analytics Application...");

        // 1. Чтение данных из Kafka
        Dataset<Row> kafkaData = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-0:1092,kafka-1:2092")
                .option("subscribe", "products")
                .option("startingOffsets", "latest")
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                "username=\"admin\" password=\"admin\";")
                .option("kafka.ssl.truststore.location", "/etc/kafka/secrets/kafka.truststore.jks")
                .option("kafka.ssl.truststore.password", "password")
                .option("kafka.ssl.keystore.location", "/etc/kafka/secrets/kafka.keystore.pkcs12")
                .option("kafka.ssl.keystore.password", "password")
                .load();

        // Схема для парсинга JSON данных (исправленный конструктор)
        StructType productSchema = new StructType(new StructField[]{
                new StructField("product_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("category", DataTypes.StringType, true, Metadata.empty()),
                new StructField("price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("rating", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty())
        });

        // 2. Парсинг JSON и преобразование данных
        Dataset<Row> products = kafkaData
                .selectExpr("CAST(value AS STRING) as json_value")
                .filter(functions.col("json_value").isNotNull())
                .select(functions.from_json(functions.col("json_value"), productSchema).as("data"))
                .select("data.*")
                .filter(functions.col("product_id").isNotNull())
                .withColumn("processing_time", functions.current_timestamp());


        // Аналитика 1: Статистика по категориям
        Dataset<Row> categoryStats = products
                .groupBy("category")
                .agg(
                        functions.count("product_id").as("product_count"),
                        functions.avg("price").as("avg_price"),
                        functions.avg("rating").as("avg_rating"),
                        functions.max("price").as("max_price"),
                        functions.min("price").as("min_price")
                )
                .withColumn("analysis_type", functions.lit("category_statistics"))
                .withColumn("timestamp", functions.current_timestamp());

        // Аналитика 2: Топ продуктов по рейтингу
        Dataset<Row> topRatedProducts = products
                .filter(functions.col("rating").isNotNull())
                .orderBy(functions.col("rating").desc())
                .limit(10)
                .select(
                        functions.col("product_id"),
                        functions.col("name"),
                        functions.col("category"),
                        functions.col("rating"),
                        functions.col("price")
                )
                .withColumn("analysis_type", functions.lit("top_rated_products"))
                .withColumn("timestamp", functions.current_timestamp());

        // Аналитика 3: Ценовое распределение
        Dataset<Row> priceDistribution = products
                .withColumn("price_range",
                        functions.when(functions.col("price").lt(100), "Budget")
                                .when(functions.col("price").between(100, 500), "Medium")
                                .when(functions.col("price").between(500, 1000), "Expensive")
                                .otherwise("Premium"))
                .groupBy("price_range")
                .agg(
                        functions.count("product_id").as("product_count"),
                        functions.avg("rating").as("avg_rating")
                )
                .withColumn("analysis_type", functions.lit("price_distribution"))
                .withColumn("timestamp", functions.current_timestamp());



        // Запись сырых данных в HDFS
        StreamingQuery rawDataQuery = products
                .writeStream()
                .outputMode("append")
                .format("parquet")
                .option("path", "hdfs://namenode:9820/data/products/raw")
                .option("checkpointLocation", "hdfs://namenode:9820/checkpoints/products_raw")
                .trigger(Trigger.ProcessingTime("1 minute"))
                .start();

        // Запись аналитики в HDFS
        StreamingQuery analyticsQuery = categoryStats
                .writeStream()
                .outputMode("complete")
                .format("parquet")
                .option("path", "hdfs://namenode:9820/data/analytics/category_stats")
                .option("checkpointLocation", "hdfs://namenode:9820/checkpoints/category_stats")
                .trigger(Trigger.ProcessingTime("2 minutes"))
                .start();





        System.out.println("✅ All streaming queries started successfully");

        // Ожидание завершения
        spark.streams().awaitAnyTermination();
    }
}