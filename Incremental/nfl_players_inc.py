
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, to_date, when, size, trim
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("NFLPlayersIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# ---------- 1.  Config ----------
BRONZE_TBL   = "nfl_players"
INC_SOURCE   = "nfl_players_inc"         
SILVER_PATH  = "hdfs:///tmp/DE011025/Joe/silver/nfl_players_output"

# ---------- 2.  row-count check ----------
bronze_df  = spark.sql(f"SELECT * FROM {BRONZE_TBL}")
bronze_cnt = bronze_df.count()

try:
    inc_df = spark.sql(f"SELECT * FROM {INC_SOURCE}")
    inc_cnt = inc_df.count()
except Exception:
    inc_cnt = 0

print(f"[PLAYERS] bronze_cnt={bronze_cnt}, inc_cnt={inc_cnt}")

# ---------- 3.  rebuild only if new data ----------
if bronze_cnt < inc_cnt:
    print("[PLAYERS] New data detected – rebuilding silver & updating bronze.")

    df = inc_df    # start with raw incremental rows

    # ----- 3a  clean strings -----
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    df = df.replace("", None)
    df = df.dropDuplicates(["player_id", "years_played"])

    # ----- 3b  player_id numeric -----
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    # ----- 3c  split name -----
    df = (df.withColumn("name_split", split(col("name"), ",\\s*"))
          .withColumn("last_name",  col("name_split").getItem(0))
          .withColumn("first_name",
                      when(size(col("name_split")) > 1,
                           col("name_split").getItem(1)).otherwise(None)))

    # ----- 3d  split birth place -----
    df = (df.withColumn("bp_split", split(col("birth_place"), ",\\s*"))
          .withColumn("birth_city", col("bp_split").getItem(0))
          .withColumn("birth_state",
                      when(size(col("bp_split")) > 1,
                           col("bp_split").getItem(1)).otherwise(None)))

    # ----- 3e  validate & cast birthday -----
    df = df.filter(col("birthday").rlike("^\\d{1,2}/\\d{1,2}/\\d{2,4}$"))
    df = df.withColumn("birthday", to_date(col("birthday"), "M/d/yyyy"))
    df = df.filter(col("birthday").isNotNull())

    # ----- 3f  drop unused -----
    columns_to_drop = [
        "age", "height_inches", "high_school", "number", "position",
        "weight_lbs", "years_played", "birth_place", "name",
        "name_split", "bp_split"
    ]
    df = df.drop(*columns_to_drop)

    # ---------- 4.  WRITE ----------
    # silver (clean) parquet
    df.write.mode("overwrite").parquet(SILVER_PATH)

    # bronze (raw) overwrite so next count matches
    spark.sql(f"INSERT OVERWRITE TABLE joepostgres.{BRONZE_TBL} "
              f"SELECT * FROM joepostgres.{INC_SOURCE}")

    print("[PLAYERS] Silver & bronze updated.")

else:
    print("[PLAYERS] No new data – skipping.")

spark.stop()
