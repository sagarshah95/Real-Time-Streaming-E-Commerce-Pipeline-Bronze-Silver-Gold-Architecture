import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StringType, StructField, StructType,IntegerType, DoubleType, TimestampType

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_timestamp", StringType(), True)
])

# Final schema written to the Silver Delta table (after parsing + processed_at).
SILVER_PATH = "s3a://lakehouse/silver/ecommerce-events"
silver_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("processed_at", TimestampType(), True),
])

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

spark = SparkSession.builder\
                    .appName("SilverClean")\
                    .config("spark.cores.max", "1")\
                    .config("spark.executor.cores", "1")\
                    .config("spark.executor.memory", "512m")\
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)\
                    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)\
                    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)\
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def ensure_silver_table():
    """Create the Silver Delta table with its schema if it does not exist yet,
    so downstream streams (Gold) can attach before the first batch is written."""
    try:
        spark.read.format("delta").load(SILVER_PATH)
    except Exception:
        print(f"Bootstrapping empty Silver table at {SILVER_PATH}")
        spark.createDataFrame([], silver_schema)\
             .write.format("delta").mode("append").save(SILVER_PATH)


def read_data():
    raw_data = spark.readStream\
                .format("delta")\
                .load("s3a://lakehouse/bronze/ecommerce-events")
    df  = raw_data.select(F.from_json(F.col("value").cast("string"),schema).alias("data"))
    data_df  = df.select("data.*")
    # data_df.show()
    return data_df

def validate_data(df):
    df = df.withColumn("event_timestamp", F.col("event_timestamp").cast("timestamp"))
    df = (
        df.filter(F.col("user_id").isNotNull())
          .filter(F.col("event_timestamp").isNotNull())
          .withWatermark("event_timestamp", "5 minutes")
          .dropDuplicates(["user_id", "event_type", "event_timestamp"])
    )
    df = df.withColumn("processed_at", F.current_timestamp())
    return df

def write_silver_tables(df):
    return df.writeStream\
      .format("delta")\
      .outputMode("append")\
      .option("checkpointLocation", "s3a://lakehouse/checkpoints/silver") \
      .trigger(processingTime="30 seconds")\
      .start(SILVER_PATH)

if __name__ == "__main__":
    ensure_silver_table()
    df = read_data()
    df = validate_data(df)
    query = write_silver_tables(df)
    query.awaitTermination()
    # df.show()
    