from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergTest") \
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    ) \
    .config(
        "spark.sql.catalog.hadoop_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    ) \
    .config(
        "spark.sql.catalog.hadoop_catalog.type",
        "hadoop"
    ) \
    .config(
        "spark.sql.catalog.hadoop_catalog.warehouse",
        "hdfs://localhost:9000/user/vboxuser/warehouse"
    ) \
    .getOrCreate()

spark.sql("""
    CREATE TABLE IF NOT EXISTS hadoop_catalog.ecommerce_events (
        event_id STRING,
        user_id STRING,
        event_type STRING,
        product_id STRING,
        amount DOUBLE,
        timestamp STRING
    )
    USING iceberg
""")

print("ICEBERG TABLE CREATED SUCCESSFULLY")

spark.sql("SHOW TABLES IN hadoop_catalog").show()

spark.stop()
