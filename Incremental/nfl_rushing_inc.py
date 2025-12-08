# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLRushingIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# ---------- config ----------
BRONZE_TBL   = "nfl_rushing"
INC_SOURCE   = "nfl_rushing_inc"
SILVER_PATH  = "hdfs:///tmp/DE011025/Joe/silver/nfl_rushing_output"

# ---------- count check ----------
bronze_df  = spark.sql("SELECT * FROM {}".format(BRONZE_TBL))
bronze_cnt = bronze_df.count()

try:
    inc_df = spark.sql("SELECT * FROM {}".format(INC_SOURCE))
    inc_cnt = inc_df.count()
except Exception:
    inc_cnt = 0

print("[RUSHING] bronze_cnt={}, inc_cnt={}".format(bronze_cnt, inc_cnt))

# ---------- rebuild only if new data ----------
if bronze_cnt < inc_cnt:
    print("[RUSHING] New data detected – rebuilding silver & bronze.")

    df = inc_df

    # 1. trim strings
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))
    df = df.replace("--", "0").replace("", None)

    # 2. de-dup
    df = df.dropDuplicates()

    # 3. player_id numeric
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    # 4. split name
    df = (df.withColumn("name_split", split(col("name"), ", "))
          .withColumn("last_name",  col("name_split").getItem(0))
          .withColumn("first_name",
                      when(size(col("name_split")) > 1,
                           col("name_split").getItem(1)).otherwise(None)))
    df = df.filter(col("first_name").isNotNull())

    # 5. integer columns
    int_cols = ["rushing_attempts", "rushing_yards", "rushing_tds",
                "rushing_first_downs", "fumbles"]
    for c in int_cols:
        df = (df.withColumn(c, regexp_replace(col(c), ",", ""))
               .withColumn(c, col(c).cast(IntegerType())))

    # 6. float / special
    df = df.withColumn("yards_per_carry", col("yards_per_carry").cast(FloatType()))
    df = df.withColumn(
        "longest_rushing_run",
        regexp_replace(col("longest_rushing_run"), "T", "").cast(IntegerType())
    )

    # 7. filters
    df = df.filter(col("rushing_yards").cast(IntegerType()) >= 1000)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    # 8. drop unused
    df = df.drop("position", "rushing_attempts_per_game", "rushing_yards_per_game",
                 "percentage_of_rushing_first_downs", "rushing_more_than_20_yards",
                 "rushing_more_than_40_yards", "name", "name_split")

    # ---------- 9.  WRITE ----------
    df.write.mode("overwrite").parquet(SILVER_PATH)
    spark.sql("INSERT OVERWRITE TABLE joepostgres.{} SELECT * FROM joepostgres.{}"
              .format(BRONZE_TBL, INC_SOURCE))

    print("[RUSHING] Silver & bronze updated.")
else:
    print("[RUSHING] No new data – skipping.")

spark.stop()
