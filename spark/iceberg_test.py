from pyspark.sql import SparkSession

WAREHOUSE = "hdfs://localhost:9000/user/vboxuser/iceberg"

spark = SparkSession.builder \
    .appName("IcebergTest") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE) \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.ecommerce")
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
print("ICEBERG TABLE READY")

spark.sql("SHOW TABLES IN local.ecommerce").show()
spark.sql("SELECT COUNT(*) AS total_events FROM local.ecommerce.events").show()

spark.stop()
