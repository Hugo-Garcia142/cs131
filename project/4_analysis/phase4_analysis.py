import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType,
    TimestampType,
)

INPUT = "gs://cs131-taxi-data/parquet_data/*.parquet"
OUTPUT = "gs://cs131-taxi-data/phase4_results"

spark = (
    SparkSession.builder
    .appName("CS131Phase4Analysis")
    .getOrCreate()
)

spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

start_time = time.time()

schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True),
])

df = spark.read.schema(schema).parquet(INPUT)

df = (
    df.withColumn(
        "trip_minutes",
        (
            F.col("tpep_dropoff_datetime").cast("long")
            - F.col("tpep_pickup_datetime").cast("long")
        ) / 60.0,
    )
    .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .filter(
        (F.col("pickup_year").between(2019, 2026))
        & (F.col("trip_minutes") > 0)
        & (F.col("trip_minutes") <= 180)
        & (F.col("trip_distance") >= 0)
        & (F.col("trip_distance") <= 100)
        & (F.col("fare_amount") >= 0)
        & (F.col("total_amount") >= 0)
    )
)

yearly = (
    df.groupBy("pickup_year")
    .agg(
        F.count("*").alias("total_trips"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("trip_minutes").alias("avg_trip_minutes"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_recorded_tip"),
        F.avg("mta_tax").alias("avg_mta_tax"),
        F.avg("congestion_surcharge").alias("avg_congestion_surcharge"),
        F.avg("total_amount").alias("avg_total_amount"),
        F.sum("total_amount").alias("gross_revenue"),
    )
    .orderBy("pickup_year")
)



duration_df = (
    df.withColumn(
        "duration_bucket",
        F.when(F.col("trip_minutes") <= 5, "01: 0-5 min")
        .when(F.col("trip_minutes") <= 10, "02: 5-10 min")
        .when(F.col("trip_minutes") <= 15, "03: 10-15 min")
        .when(F.col("trip_minutes") <= 20, "04: 15-20 min")
        .when(F.col("trip_minutes") <= 25, "05: 20-25 min")
        .when(F.col("trip_minutes") <= 30, "06: 25-30 min")
        .when(F.col("trip_minutes") <= 35, "07: 30-35 min")
        .when(F.col("trip_minutes") <= 40, "08: 35-40 min")
        .otherwise("09: 40+ min")
    )
)

tip_by_duration = (
    duration_df.groupBy("duration_bucket")
    .agg(
        F.count("*").alias("total_trips"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_recorded_tip"),
        F.avg("mta_tax").alias("avg_mta_tax"),
        F.avg("tolls_amount").alias("avg_tolls"),
        F.avg("congestion_surcharge").alias("avg_congestion_surcharge"),
        F.avg("total_amount").alias("avg_total_amount"),
    )
    .orderBy("duration_bucket")
)

tip_by_hour = (
    df.groupBy("pickup_hour")
    .agg(
        F.count("*").alias("total_trips"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("trip_minutes").alias("avg_trip_minutes"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_recorded_tip"),
        F.avg("mta_tax").alias("avg_mta_tax"),
        F.avg("congestion_surcharge").alias("avg_congestion_surcharge"),
        F.avg("total_amount").alias("avg_total_amount"),
    )
    .orderBy("pickup_hour")
)

distance_df = (
    df.withColumn(
        "distance_bucket",
        F.when(F.col("trip_distance") < 1, "01: Under 1 mile")
        .when(F.col("trip_distance") < 2, "02: 1-2 miles")
        .when(F.col("trip_distance") < 4, "03: 2-4 miles")
        .when(F.col("trip_distance") < 8, "04: 4-8 miles")
        .when(F.col("trip_distance") < 16, "05: 8-16 miles")
        .when(F.col("trip_distance") < 32, "06: 16-32 miles")
        .otherwise("07: 32+ miles")
    )
)

tip_by_distance = (
    distance_df.groupBy("distance_bucket")
    .agg(
        F.count("*").alias("total_trips"),
        F.avg("trip_minutes").alias("avg_trip_minutes"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_recorded_tip"),
        F.avg("tolls_amount").alias("avg_tolls"),
        F.avg("congestion_surcharge").alias("avg_congestion_surcharge"),
        F.avg("total_amount").alias("avg_total_amount"),
    )
    .orderBy("distance_bucket")
)
print("=== YEARLY ===")
yearly.show(20, truncate=False)

print("=== TIP_BY_DURATION ===")
tip_by_duration.show(20, truncate=False)

print("=== TIP_BY_HOUR ===")
tip_by_hour.show(24, truncate=False)

print("=== TIP_BY_DISTANCE ===")
tip_by_distance.show(20, truncate=False)

tables = {
    "yearly": yearly,
    "tip_by_duration": tip_by_duration,
    "tip_by_hour": tip_by_hour,
    "tip_by_distance": tip_by_distance,
}

for name, table in tables.items():
    (
        table.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(f"{OUTPUT}/{name}")
    )

elapsed = time.time() - start_time

print(f"METRIC|input_files|{len(df.inputFiles())}")
print(f"METRIC|execution_seconds|{elapsed:.2f}")
print(f"METRIC|output_path|{OUTPUT}")

spark.stop()
