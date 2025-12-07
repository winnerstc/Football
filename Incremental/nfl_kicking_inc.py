# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLKickingIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

bronze_table = "nfl_kicking"
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_kicking_output"

bronze_df = spark.sql("SELECT * FROM " + bronze_table)
bronze_count = bronze_df.count()

try:
    silver_df = spark.read.parquet(silver_path)
    silver_count = silver_df.count()
except Exception:
    silver_count = 0

print("[KICKING] bronze_count=" + str(bronze_count)
      + ", silver_count=" + str(silver_count))

if bronze_count > silver_count:
    print("[KICKING] New data detected. Rebuilding silver...")

    df = bronze_df

    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    df = df.replace("--", "0")
    df = df.replace("", None)

    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("last_name", col("name_split")[0])
    df = df.withColumn(
        "first_name",
        when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None)
    )
    df = df.filter(col("first_name").isNotNull())

    int_cols = [
        "kicks_blocked",
        "longest_fg_made",
        "fgs_made",
        "fgs_attempted",
        "fgs_made_2029_yards",
        "fgs_attempted_2029_yards",
        "fgs_made_3039_yards",
        "fgs_attempted_3039_yards",
        "fgs_made_4049_yards",
        "fgs_attempted_4049_yards",
        "fgs_made_50_yards",
        "fgs_attempted_50_yards",
        "extra_points_attempted",
        "extra_points_made"
    ]

    for c in int_cols:
        df = df.withColumn(c, regexp_replace(col(c), ",", ""))
        df = df.withColumn(c, regexp_replace(col(c), "[^0-9-]", ""))
        df = df.withColumn(c, col(c).cast(IntegerType()))

    float_cols = [
        "fg_percentage",
        "fgs_percentage_2029_yards",
        "fgs_percentage_3039_yards",
        "fgs_percentage_4049_yards",
        "fgs_percentage_50_yards",
        "percentage_of_extra_points"
    ]

    for c in float_cols:
        df = df.withColumn(c, col(c).cast(FloatType()))

    df = df.filter(col("fgs_made") >= 20)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    df = df.drop(
        "position",
        "kicks_blocked",
        "extra_points_blocked",
        "name",
        "name_split"
    )

    df.write.mode("overwrite").parquet(silver_path)
    print("[KICKING] Silver refreshed.")

else:
    print("[KICKING] No new data. Skipping rebuild.")

spark.stop()
