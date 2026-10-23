from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

KAFKA_SERVERS = "localhost:9092"
TOPIC = "ecommerce-events"
WAREHOUSE = "hdfs://localhost:9000/user/vboxuser/iceberg"
CHECKPOINT = "hdfs://localhost:9000/user/vboxuser/iceberg_checkpoint"
TABLE = "local.ecommerce.events"

spark = SparkSession.builder \
    .appName("KafkaSparkIcebergStreaming") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE) \
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
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.ecommerce")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        event_id STRING,
        user_id STRING,
        event_type STRING,
        product_id STRING,
        amount DOUBLE,
        timestamp STRING
    ) USING iceberg
""")

# Kafka -> Spark -> Iceberg
query = parsed_df.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT) \
    .toTable(TABLE)

query.awaitTermination()
