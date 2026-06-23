from pyspark.sql import SparkSession
import os


MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")

spark = SparkSession.builder\
                    .appName("Gold_data")\
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


def count_layer(name, path):
    try:
        count = spark.read.format("delta").load(path).count()
        print(f"{name}: {count} rows")
    except Exception as e:
        msg = str(e)
        if "PATH_NOT_FOUND" in msg or "Path does not exist" in msg:
            print(f"{name}: not created yet (no data written to {path})")
        else:
            print(f"{name}: could not read ({msg.splitlines()[0]})")


count_layer("bronze_layer", "s3a://lakehouse/bronze/ecommerce-events")
count_layer("silver_layer", "s3a://lakehouse/silver/ecommerce-events")
count_layer("gold_layer", "s3a://lakehouse/gold/kpi-metrices")

