import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("CS131Phase3TaxiAnalysis") \
    .getOrCreate()

gcs_input_path = "gs://cs131-taxi-data/data/may_of_each_year_sample_2019_to_2026.csv"

start_time = time.time()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(gcs_input_path)

aggregated_df = df.filter(F.col("payment_type").isin(1, 2)) \
    .groupBy("payment_type") \
    .agg(
        F.count("*").alias("total_trips"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_tip"),
        F.sum("total_amount").alias("gross_revenue")
    ) \
    .orderBy("payment_type")

aggregated_df.show()

end_time = time.time()
print(f"=== PYSPARK EXECUTION TIME: {end_time - start_time:.2f} SECONDS ===")

spark.stop()
