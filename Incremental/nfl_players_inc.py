

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, to_date, when, size, trim
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("NFLPlayersIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

bronze_table = "nfl_players"
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_players_output"
bronze_path="hdfs:///tmp/DE011025/Joe/raw/nfl_players"

bronze_df = spark.sql("SELECT * FROM " + bronze_table)
bronze_count = bronze_df.count()

try:
    inc_df = spark.sql("SELECT * FROM nfl_players_inc")
    inc_count = inc_df.count()
except Exception:
    inc_count = 0

print("[PLAYERS] bronze_count=" + str(bronze_count)
      + ", incremental_count=" + str(inc_count))

if bronze_count < inc_count:
    print("[PLAYERS] New data found. Rebuilding silver...")

    df = inc_df

    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    df = df.replace("", None)

    df = df.dropDuplicates(["player_id", "years_played"])

    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    df = df.withColumn("name_split", split(col("name"), ",\\s*"))
    df = df.withColumn("last_name", col("name_split").getItem(0))
    df = df.withColumn(
        "first_name",
        when(size(col("name_split")) > 1, col("name_split").getItem(1)).otherwise(None)
    )

    df = df.withColumn("bp_split", split(col("birth_place"), ",\\s*"))
    df = df.withColumn("birth_city", col("bp_split").getItem(0))
    df = df.withColumn(
        "birth_state",
        when(size(col("bp_split")) > 1, col("bp_split").getItem(1)).otherwise(None)
    )

    df = df.filter(col("birthday").rlike("^\\d{1,2}/\\d{1,2}/\\d{2,4}$"))
    df = df.withColumn("birthday", to_date(col("birthday"), "M/d/yyyy"))
    df = df.filter(col("birthday").isNotNull())

    columns_to_drop = [
        "age", "height_inches", "high_school", "number", "position",
        "weight_lbs", "years_played", "birth_place", "name",
        "name_split", "bp_split"
    ]
    df = df.drop(*columns_to_drop)

    df.write.mode("overwrite").parquet(silver_path)
    print("[PLAYERS] Silver refreshed.")
    inc_df.write.mode("overwrite").parquet("bronze_path")

else:
    print("[PLAYERS] No new data. Skipping.")

spark.stop()
