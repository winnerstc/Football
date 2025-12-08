# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLKickingIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# ---------- config ----------
BRONZE_TBL   = "nfl_kicking"
INC_SOURCE   = "nfl_kicking_inc"
SILVER_PATH  = "hdfs:///tmp/DE011025/Joe/silver/nfl_kicking_output"

# ---------- count check ----------
bronze_df  = spark.sql("SELECT * FROM {}".format(BRONZE_TBL))
bronze_cnt = bronze_df.count()

try:
    inc_df = spark.sql("SELECT * FROM {}".format(INC_SOURCE))
    inc_cnt = inc_df.count()
except Exception:
    inc_cnt = 0

print("[KICKING] bronze_cnt={}, inc_cnt={}".format(bronze_cnt, inc_cnt))

# ---------- rebuild only if new data ----------
if bronze_cnt < inc_cnt:
    print("[KICKING] New data detected – rebuilding silver & bronze.")

    df = inc_df

    # 1. trim strings
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))
    df = df.replace("--", "0").replace("", None)

    # 2. player_id numeric
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    # 3. split name
    df = (df.withColumn("name_split", split(col("name"), ", "))
          .withColumn("last_name",  col("name_split").getItem(0))
          .withColumn("first_name",
                      when(size(col("name_split")) > 1,
                           col("name_split").getItem(1)).otherwise(None)))
    df = df.filter(col("first_name").isNotNull())

    # 4. integer columns
    int_cols = [
        "kicks_blocked", "longest_fg_made", "fgs_made", "fgs_attempted",
        "fgs_made_2029_yards", "fgs_attempted_2029_yards",
        "fgs_made_3039_yards", "fgs_attempted_3039_yards",
        "fgs_made_4049_yards", "fgs_attempted_4049_yards",
        "fgs_made_50_yards", "fgs_attempted_50_yards",
        "extra_points_attempted", "extra_points_made"
    ]
    for c in int_cols:
        df = (df.withColumn(c, regexp_replace(col(c), ",", ""))
               .withColumn(c, regexp_replace(col(c), "[^0-9-]", ""))
               .withColumn(c, col(c).cast(IntegerType())))

    # 5. float columns
    float_cols = [
        "fg_percentage", "fgs_percentage_2029_yards", "fgs_percentage_3039_yards",
        "fgs_percentage_4049_yards", "fgs_percentage_50_yards",
        "percentage_of_extra_points"
    ]
    for c in float_cols:
        df = df.withColumn(c, col(c).cast(FloatType()))

    # 6. filters
    df = df.filter(col("fgs_made").cast(IntegerType()) >= 20)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    # 7. drop unused
    df = df.drop("position", "kicks_blocked", "extra_points_blocked", "name", "name_split")

    # ---------- 8.  WRITE ----------
    # silver (clean)
    df.write.mode("overwrite").parquet(SILVER_PATH)

    # bronze (raw) overwrite
    spark.sql("INSERT OVERWRITE TABLE joepostgres.{} SELECT * FROM joepostgres.{}"
              .format(BRONZE_TBL, INC_SOURCE))

    print("[KICKING] Silver & bronze updated.")
else:
    print("[KICKING] No new data – skipping.")

spark.stop()
