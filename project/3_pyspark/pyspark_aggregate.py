import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CS131Phase3FullParquetAnalysis") \
    .getOrCreate()

# Path matching ALL Parquet files in GCS
gcs_parquet_path = "gs://cs131-taxi-data/parquet_data/*.parquet"

start_time = time.time()

# 1. Read All Parquet Files concurrently via Columnar Reader
df = spark.read.parquet(gcs_parquet_path)

# 2. Perform Grouped Aggregations
aggregated_df = df.filter(F.col("payment_type").isin(1, 2)) \
    .groupBy("payment_type") \
    .agg(
        F.count("*").alias("total_trips"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_tip"),
        F.sum("total_amount").alias("gross_revenue")
    ) \
    .orderBy("payment_type")

# 3. Output Trigger
aggregated_df.show()

end_time = time.time()
print(f"=== PYSPARK PARQUET EXECUTION TIME: {end_time - start_time:.2f} SECONDS ===")

spark.stop()
