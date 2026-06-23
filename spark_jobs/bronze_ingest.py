from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# from event_schema import schema
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
TOPIC = os.getenv("TOPIC")


spark = SparkSession.builder \
    .appName("BronzeIngest") \
    .config("spark.cores.max", "1")\
    .config("spark.executor.cores", "1")\
    .config("spark.executor.memory", "512m")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
    "subscribe": TOPIC,
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}


def read_raw_data():
    raw_df = spark.readStream\
         .format("kafka")\
         .options(**kafka_options)\
         .load()
    return raw_df

def write_bronze(df):
  return df.writeStream\
           .format("delta")\
           .outputMode("append")\
           .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze")\
           .trigger(processingTime="10 seconds")\
           .start("s3a://lakehouse/bronze/ecommerce-events")

if __name__ == "__main__":
    raw_df = read_raw_data()
    query = write_bronze(raw_df)
    query.awaitTermination()
