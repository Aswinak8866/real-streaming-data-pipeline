from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaSparkIcebergStreaming") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "hdfs://localhost:9000/user/vboxuser/iceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", StringType()) \
    .add("event_type", StringType()) \
    .add("product_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("timestamp", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce-events") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr(
    "CAST(value AS STRING) AS json"
)

parsed_df = json_df.select(
    from_json(col("json"), schema).alias("data")
).select("data.*")

# Create Iceberg table
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.ecommerce.events (
        event_id STRING,
        user_id STRING,
        event_type STRING,
        product_id STRING,
        amount DOUBLE,
        timestamp STRING
    ) USING iceberg
""")

# Kafka → Spark → Iceberg
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("iceberg") \
    .option("path", "hdfs://localhost:9000/user/vboxuser/iceberg/events") \
    .option("checkpointLocation", "hdfs://localhost:9000/user/vboxuser/iceberg_checkpoint") \
    .toTable("local.ecommerce.events")

query.awaitTermination()
