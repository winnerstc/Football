# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLDefensiveIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# ---------- config ----------
BRONZE_TBL   = "nfl_defensive"
INC_SOURCE   = "nfl_defensive_inc"
SILVER_PATH  = "hdfs:///tmp/DE011025/Joe/silver/nfl_defensive_output"

# ---------- count check ----------
bronze_df  = spark.sql("SELECT * FROM {}".format(BRONZE_TBL))
bronze_cnt = bronze_df.count()

try:
    inc_df = spark.sql("SELECT * FROM {}".format(INC_SOURCE))
    inc_cnt = inc_df.count()
except Exception:
    inc_cnt = 0

print("[DEFENSIVE] bronze_cnt={}, inc_cnt={}".format(bronze_cnt, inc_cnt))

# ---------- rebuild only if new data ----------
if bronze_cnt < inc_cnt:
    print("[DEFENSIVE] New data detected – rebuilding silver & bronze.")

    df = inc_df

    # 1. trim strings
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    # 2. placeholders
    df = df.replace("--", "0").replace("", None)

    # 3. de-dup
    df = df.dropDuplicates()

    # 4. player_id numeric
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    # 5. split name
    df = (df.withColumn("name_split", split(col("name"), ", "))
          .withColumn("last_name",  col("name_split").getItem(0))
          .withColumn("first_name",
                      when(size(col("name_split")) > 1,
                           col("name_split").getItem(1)).otherwise(None)))
    df = df.filter(col("first_name").isNotNull())

    # 6. integer columns
    int_cols = ["solo_tackles", "assisted_tackles", "passes_defended",
                "ints", "ints_for_tds", "int_yards", "safties"]
    for c in int_cols:
        df = (df.withColumn(c, regexp_replace(col(c), ",", ""))
               .withColumn(c, col(c).cast(IntegerType())))

    # 7. float + filter
    df = (df.withColumn("sacks", col("sacks").cast(FloatType()))
           .withColumn("total_tackles", col("total_tackles").cast(FloatType())))
    df = df.filter(col("total_tackles") >= 100)

    # 8. drop unused
    df = df.drop("position", "yards_per_int", "longest_int_return",
                 "name", "name_split")

    # ---------- 9.  WRITE ----------
    # silver (clean)
    df.write.mode("overwrite").parquet(SILVER_PATH)

    # bronze (raw) overwrite
    spark.sql("INSERT OVERWRITE TABLE joepostgres.{} SELECT * FROM joepostgres.{}"
              .format(BRONZE_TBL, INC_SOURCE))

    print("[DEFENSIVE] Silver & bronze updated.")
else:
    print("[DEFENSIVE] No new data – skipping.")

spark.stop()
