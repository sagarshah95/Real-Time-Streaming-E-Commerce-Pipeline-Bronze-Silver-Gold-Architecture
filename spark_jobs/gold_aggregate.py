from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import time 

MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")

spark = SparkSession.builder\
                    .appName("GoldAggregate")\
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
def read_silver_data():
    df = spark.readStream\
              .format("delta")\
              .load("s3a://lakehouse/silver/ecommerce-events")\
            #   .load()
    return df

def aggregated_data(df):
    gold_agg = (
                 df.withWatermark("event_timestamp", "10 minutes")
                   .groupby(F.window("event_timestamp", "5 minutes"))
                   .agg(
                       F.count("*").alias("total_events"),
                       F.approx_count_distinct("user_id").alias("unique_users"),
                       F.sum(F.when(F.col("event_type") == "purchase", F.col("amount")).otherwise(F.lit(0))).alias("total_revenue"),
                       F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
                       F.avg(F.when(F.col("event_type") == "purchase", F.col("amount"))).alias("avg_order_value")
                   )
                   .withColumn("conversion_rate", F.col("purchase_count")/F.col("total_events"))
                   .withColumn("window_start", F.col("window.start"))
                   .withColumn("window_end", F.col("window.end"))
                   .drop("window")

    )
    return gold_agg

def write_gold(df):
    return df.writeStream.format("delta")\
            .outputMode("complete")\
            .option("checkpointLocation", "s3a://lakehouse/checkpoints/gold")\
            .trigger(processingTime="60 seconds")\
            .start("s3a://lakehouse/gold/kpi-metrices")
    

if __name__ == "__main__":
    df = read_silver_data()
    gold_agg = aggregated_data(df)
    query = write_gold(gold_agg)
    print(query.status)
    while query.isActive:
       print(query.lastProgress)
       time.sleep(30)
    query.awaitTermination()