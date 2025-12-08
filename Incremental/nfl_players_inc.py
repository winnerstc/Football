# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, to_date, when, size, trim
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("NFLPlayersIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# ---------- config ----------
bronze_table     = "nfl_players"
inc_source_table = "nfl_players_inc"
silver_path      = "hdfs:///tmp/DE011025/Joe/silver/nfl_players_output"

# ---------- count check ----------
bronze_df  = spark.sql("SELECT * FROM {}".format(bronze_table))
bronze_cnt = bronze_df.count()

try:
    inc_df = spark.sql("SELECT * FROM {}".format(inc_source_table))
    inc_cnt = inc_df.count()
except Exception:
    inc_cnt = 0

print("[PLAYERS] bronze_cnt={}, inc_cnt={}".format(bronze_cnt, inc_cnt))

# ---------- rebuild only if new data ----------
if bronze_cnt < inc_cnt:
    print("[PLAYERS] New data found – rebuilding silver & bronze.")

    df = inc_df

    # string clean
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))
    df = df.replace("", None)
    df = df.dropDuplicates(["player_id", "years_played"])

    # player_id numeric
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    # split name
    df = (df.withColumn("name_split", split(col("name"), ",\\s*"))
          .withColumn("last_name",  col("name_split").getItem(0))
          .withColumn("first_name",
                      when(size(col("name_split")) > 1,
                           col("name_split").getItem(1)).otherwise(None)))

    # split birth place
    df = (df.withColumn("bp_split", split(col("birth_place"), ",\\s*"))
          .withColumn("birth_city", col("bp_split").getItem(0))
          .withColumn("birth_state",
                      when(size(col("bp_split")) > 1,
                           col("bp_split").getItem(1)).otherwise(None)))

    # birthday validate
    df = df.filter(col("birthday").rlike("^\\d{1,2}/\\d{1,2}/\\d{2,4}$"))
    df = df.withColumn("birthday", to_date(col("birthday"), "M/d/yyyy"))
    df = df.filter(col("birthday").isNotNull())

    # drop unused
    df = df.drop("age", "height_inches", "high_school", "number", "position",
                 "weight_lbs", "years_played", "birth_place", "name",
                 "name_split", "bp_split")

    # write
    df.write.mode("overwrite").parquet(silver_path)
    spark.sql("INSERT OVERWRITE TABLE joepostgres.{} SELECT * FROM joepostgres.{}"
              .format(bronze_table, inc_source_table))

    print("[PLAYERS] Silver & bronze updated.")
else:
    print("[PLAYERS] No new data – skipping.")

spark.stop()
