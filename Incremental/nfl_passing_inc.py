# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLPassingIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# ---------- config ----------
BRONZE_TBL   = "nfl_passing"
INC_SOURCE   = "nfl_passing_inc"
SILVER_PATH  = "hdfs:///tmp/DE011025/Joe/silver/nfl_passing_output"

# ---------- count check ----------
bronze_df  = spark.sql("SELECT * FROM {}".format(BRONZE_TBL))
bronze_cnt = bronze_df.count()

try:
    inc_df = spark.sql("SELECT * FROM {}".format(INC_SOURCE))
    inc_cnt = inc_df.count()
except Exception:
    inc_cnt = 0

print("[PASSING] bronze_cnt={}, inc_cnt={}".format(bronze_cnt, inc_cnt))

# ---------- rebuild only if new data ----------
if bronze_cnt < inc_cnt:
    print("[PASSING] New data detected – rebuilding silver & bronze.")

    df = inc_df

    # 1. trim strings
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))
    df = df.replace("", None)
    df = df.dropDuplicates()

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
                           col("name_split").getItem(1))))
    df = df.filter(col("first_name").isNotNull())

    # 4. numeric columns
    dash_cols = ["passes_attempted", "passes_completed", "completion_percentage",
                 "passing_yards", "passing_yards_per_attempt",
                 "td_passes", "ints", "sacks"]
    for c in dash_cols:
        df = df.withColumn(c, when(col(c) == "--", "0").otherwise(col(c)))

    int_cols = ["passes_attempted", "passes_completed",
                "passing_yards", "td_passes", "ints"]
    for c in int_cols:
        df = (df.withColumn(c, regexp_replace(col(c), ",", ""))
               .withColumn(c, col(c).cast(IntegerType())))

    float_cols = ["completion_percentage", "passing_yards_per_attempt", "sacks"]
    for c in float_cols:
        df = df.withColumn(c, col(c).cast(FloatType()))

    # 5. business filters
    df = df.filter(col("passing_yards").cast(IntegerType()) >= 1000)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    # 6. drop unused
    df = df.drop("position", "pass_attempts_per_game", "passing_yards_per_game",
                 "percentage_of_tds_per_attempts", "int_rate", "longest_pass",
                 "passes_longer_than_20_yards", "passes_longer_than_40_yards",
                 "sacked_yards_lost", "name", "name_split")

    # ---------- 7.  WRITE ----------
    df.write.mode("overwrite").parquet(SILVER_PATH)
    spark.sql("INSERT OVERWRITE TABLE joepostgres.{} SELECT * FROM joepostgres.{}"
              .format(BRONZE_TBL, INC_SOURCE))

    print("[PASSING] Silver & bronze updated.")
else:
    print("[PASSING] No new data – skipping.")

spark.stop()
