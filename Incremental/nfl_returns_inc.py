# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("NFLReturnsIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# ---------- config ----------
BRONZE_TBL   = "nfl_returns"
INC_SOURCE   = "nfl_returns_inc"
SILVER_PATH  = "hdfs:///tmp/DE011025/Joe/silver/nfl_returns_output"

# ---------- count check ----------
bronze_df  = spark.sql("SELECT * FROM {}".format(BRONZE_TBL))
bronze_cnt = bronze_df.count()

try:
    inc_df = spark.sql("SELECT * FROM {}".format(INC_SOURCE))
    inc_cnt = inc_df.count()
except Exception:
    inc_cnt = 0

print("[RETURNS] bronze_cnt={}, inc_cnt={}".format(bronze_cnt, inc_cnt))

# ---------- rebuild only if new data ----------
if bronze_cnt < inc_cnt:
    print("[RETURNS] New data detected – rebuilding silver & bronze.")

    df = inc_df

    # 1. trim strings
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    # 2. placeholders
    dash_cols = ["kick_returns", "yards_kick_returned", "kick_returns_for_tds",
                 "punt_returns", "yards_punt_returned", "punt_returns_for_tds"]
    df = df.replace("--", "0", subset=dash_cols).replace("", None)

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
    for c in dash_cols:
        df = (df.withColumn(c, regexp_replace(col(c), ",", ""))
               .withColumn(c, col(c).cast(IntegerType())))

    # 7. filters
    df = df.filter(col("kick_returns_for_tds") >= 1)
    df = df.filter(col("punt_returns_for_tds") >= 1)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    # 8. drop unused
    df = df.drop("name", "name_split")

    # ---------- 9.  WRITE ----------
    df.write.mode("overwrite").parquet(SILVER_PATH)
    spark.sql("INSERT OVERWRITE TABLE joepostgres.{} SELECT * FROM joepostgres.{}"
              .format(BRONZE_TBL, INC_SOURCE))

    print("[RETURNS] Silver & bronze updated.")
else:
    print("[RETURNS] No new data – skipping.")

spark.stop()
