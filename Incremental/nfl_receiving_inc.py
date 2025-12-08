# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLReceivingIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# ---------- config ----------
BRONZE_TBL   = "nfl_receiving"
INC_SOURCE   = "nfl_receiving_inc"
SILVER_PATH  = "hdfs:///tmp/DE011025/Joe/silver/nfl_receiving_output"

# ---------- count check ----------
bronze_df  = spark.sql("SELECT * FROM {}".format(BRONZE_TBL))
bronze_cnt = bronze_df.count()

try:
    inc_df = spark.sql("SELECT * FROM {}".format(INC_SOURCE))
    inc_cnt = inc_df.count()
except Exception:
    inc_cnt = 0

print("[RECEIVING] bronze_cnt={}, inc_cnt={}".format(bronze_cnt, inc_cnt))

# ---------- rebuild only if new data ----------
if bronze_cnt < inc_cnt:
    print("[RECEIVING] New data detected – rebuilding silver & bronze.")

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
    int_cols = ["receptions", "receiving_yards", "receiving_tds",
                "first_down_receptions", "fumbles"]
    for c in int_cols:
        df = (df.withColumn(c, regexp_replace(col(c), ",", ""))
               .withColumn(c, col(c).cast(IntegerType())))

    # 5. float / special
    df = df.withColumn("yards_per_reception", col("yards_per_reception").cast(FloatType()))
    df = df.withColumn(
        "longest_reception",
        regexp_replace(col("longest_reception"), "T", "").cast(IntegerType())
    )

    # 6. filters
    df = df.filter(col("receiving_yards").cast(IntegerType()) >= 1000)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    # 7. drop unused
    df = df.drop("position", "yards_per_reception", "yards_per_game",
                 "receptions_longer_than_20_yards", "receptions_longer_than_40_yards",
                 "first_down_receptions", "name", "name_split")

    # ---------- 8.  WRITE ----------
    df.write.mode("overwrite").parquet(SILVER_PATH)
    spark.sql("INSERT OVERWRITE TABLE joepostgres.{} SELECT * FROM joepostgres.{}"
              .format(BRONZE_TBL, INC_SOURCE))

    print("[RECEIVING] Silver & bronze updated.")
else:
    print("[RECEIVING] No new data – skipping.")

spark.stop()
