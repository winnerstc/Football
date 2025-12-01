"""
Author: Marc Hanna
Description: ETL script to process kick return stats CSV into Star Schema (Raw → Silver → Gold) using PySpark.
"""
import os
import shutil

from pyspark.sql import functions as F
##### Initialize Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, coalesce, when,
    sum as _sum, min as _min, max as _max, round as _round, lit
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

metastore_path = "/home/Consultants/metastore_db"

#set SparkSession config to correct hadoop warehouse
spark = SparkSession.builder \
    .appName("FootballKickReturnsApp") \
    .config("spark.sql.warehouse.dir", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://ip-172-31-8-235.eu-west-2.compute.internal:9083") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

### Create external tables if not exist
## HDFS locations (for EXTERNAL tables)
# bronze_path = "hdfs:///tmp/DE011025/marc/raw/"
# silver_path = "hdfs:///tmp/DE011025/marc/silver/"
# gold_path_base = "hdfs:///tmp/DE011025/marc/gold/"

# spark.sql(f"""
# CREATE EXTERNAL TABLE IF NOT EXISTS {bronze_table} (
#     player_id STRING,
#     name STRING,
#     position STRING,
#     year INT,
#     team STRING,
#     games_played INT,
#     returns INT,
#     yards_returned INT,
#     yards_per_return DOUBLE,
#     longest_return INT,
#     returns_for_tds INT,
#     returns_longer_20 INT,
#     returns_longer_40 INT,
#     fair_catches INT,
#     fumbles INT
# )
# ROW FORMAT DELIMITED
# FIELDS TERMINATED BY ','
# STORED AS TEXTFILE
# LOCATION '{bronze_path}'
# TBLPROPERTIES ("skip.header.line.count"="1");""")
#
## Save the staging data to the Bronze Hive table
# staging_df.write.mode("overwrite").format("hive").option("path", bronze_path).saveAsTable(bronze_table)

##### 1ST PART local testing:
kick_returns_schema = StructType([
    StructField("player_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("position", StringType(), True),
    StructField("year", IntegerType(), False),
    StructField("team", StringType(), False),
    StructField("games_played", IntegerType(), False),
    StructField("returns", IntegerType(), True),
    StructField("yards_returned", IntegerType(), True),
    StructField("yards_per_return", DoubleType(), True),
    StructField("longest_return", IntegerType(), True),
    StructField("returns_for_tds", IntegerType(), True),
    StructField("returns_longer_20", IntegerType(), True),
    StructField("returns_longer_40", IntegerType(), True),
    StructField("fair_catches", IntegerType(), True),
    StructField("fumbles", IntegerType(), True)
])

raw_data = "Career_Stats_Kick_Return.csv"
staging_df = spark.read \
    .option("header", "true") \
    .schema(kick_returns_schema) \
    .csv(raw_data)


##### 1ST PART PRODUCTION
## HDFS testing
# staging_df = spark.table("default.marc_bronze_kick_return")
# clean_df = spark.table("marc_silver_kick_return")

##### Bronze -> Silver: Clean/Rename/Standardize (NOT counted as TRANSFORMATIONs)
from pyspark.sql.functions import col, coalesce, lit, upper, trim, when
clean_df = staging_df \
    .withColumn("team_std", upper(trim(col("team")))) \
    .withColumn("fair_catches_clean", coalesce(col("fair_catches"), lit(0))) \
    .withColumn("returns_clean", coalesce(col("returns"), lit(0))) \
    .withColumn("yards_returned_clean", coalesce(col("yards_returned"), lit(0))) \
    .withColumn("yards_per_return_clean", coalesce(col("yards_per_return"), lit(0))) \
    .withColumn("returns_longer_20_clean", coalesce(col("returns_longer_20"), lit(0))) \
    .withColumn("returns_longer_40_clean", coalesce(col("returns_longer_40"), lit(0))) \
    .withColumn("returns_for_tds_clean", coalesce(col("returns_for_tds"), lit(0))) \
    .withColumn("fumbles_clean", coalesce(col("fumbles"), lit(0))) \
    .withColumn("longest_return_clean", coalesce(col("longest_return"), lit(0))) \
    .withColumn("long_return_flag", when(coalesce(col("longest_return"), lit(0)) > 40, 1).otherwise(0)) \
    .withColumn("position_std", upper(trim(col("position"))))

##### 1ST PART PRODUCTION
# Save updated Silver with correct order columns to HDFS and table
silver_cols = spark.table("marc_silver_kick_return").columns
clean_df = clean_df.select(silver_cols)
clean_df.write.mode("overwrite").insertInto("marc_silver_kick_return")

##### Build dim_player
player_agg = clean_df.groupBy("player_id","name", "position_std") \
    .agg(
        # compute min/max years and totals (used to derive TRANSFORMATION 1)
        _min("year").alias("debut_year"),
        _max("year").alias("last_year"),
        _sum("yards_returned_clean").alias("total_yards"),
        _sum("returns_clean").alias("total_returns")
    )

# Silver -> Gold: break data into star schema tables and perform transformations
# TRANSFORMATION 1: career_span = last_year - debut_year + 1
player_metrics = player_agg.withColumn(
    "career_span",
    (col("last_year") - col("debut_year") + 1)
)

# TRANSFORMATION 2: rookie_numeric = 1 if career_span == 1 else 0
player_metrics = player_metrics.withColumn(
    "rookie_numeric",
    when(col("career_span") == 1, 1).otherwise(0)
)

# TRANSFORMATION 3: avg_yards_per_year = total_yards / career_span
player_metrics = player_metrics.withColumn(
    "avg_yards_per_year",
    _round(col("total_yards") / when(col("career_span") == 0, 1).otherwise(col("career_span")), 2)
)

# TRANSFORMATION 4: avg_returns_per_year = total_returns / career_span
player_metrics = player_metrics.withColumn(
    "avg_returns_per_year",
    _round(col("total_returns") / when(col("career_span") == 0, 1).otherwise(col("career_span")), 2)
)
# Assign player surrogate key
window_player = Window.orderBy("player_id")
dim_player = player_metrics.withColumn("player_key", row_number().over(window_player))

##### Build dim_team
# Assign team surrogate key
window_team = Window.orderBy("team_std")
dim_team = clean_df.select("team_std").distinct().withColumn("team_key", row_number().over(window_team))


##### Build dim_year
dim_year = clean_df.select("year").distinct() \
    .withColumn("year_key", row_number().over(Window.orderBy("year")))

##### Create fact_kick_return_stats and compute business-value metrics
# join staging to dimension keys
s = clean_df.alias("s")
p = dim_player.alias("p")
t = dim_team.alias("t")
y = dim_year.alias("y")
joined = s.join(p, s.player_id == p.player_id, how="left") \
          .join(t, s.team_std == t.team_std, how="left") \
          .join(y, s.year == y.year, how="left")

# Create fact table from staged
fact_df = joined.select(
    col("p.player_key"),
    col("t.team_key"),
    col("y.year_key"),
    col("s.games_played"),
    col("s.returns_clean").alias("returns"),
    col("s.yards_returned_clean").alias("yards_returned"),
    col("s.yards_per_return_clean").alias("yards_per_return"),
    col("s.returns_longer_20_clean").alias("returns_longer_20"),
    col("s.returns_longer_40_clean").alias("returns_longer_40"),
    col("s.returns_for_tds_clean").alias("returns_for_tds"),
    col("s.fumbles_clean").alias("fumbles")
)

# TRANSFORMATION 5: success_rate_20plus = returns_longer_20 / returns
fact_df = fact_df.withColumn(
    "success_rate_20plus",
    when(col("returns") > 0, col("returns_longer_20") / col("returns")).otherwise(0)
)

# TRANSFORMATION 6: success_rate_40plus = returns_longer_40 / returns
fact_df = fact_df.withColumn(
    "success_rate_40plus",
    when(col("returns") > 0, col("returns_longer_40") / col("returns")).otherwise(0)
)

# TRANSFORMATION 7: turnover_risk = fumbles / returns
fact_df = fact_df.withColumn(
    "turnover_risk",
    when(col("returns") > 0, col("fumbles") / col("returns")).otherwise(0)
)

# TRANSFORMATION 8: weighted_return_score = yards_returned * returns_for_tds
fact_df = fact_df.withColumn(
    "weighted_return_score",
    col("yards_returned") * col("returns_for_tds")
)

# TRANSFORMATION 9: cumulative_yards per player (running total by year)
window_cum = Window.partitionBy("player_key").orderBy("year_key").rowsBetween(Window.unboundedPreceding, 0)
fact_df = fact_df.withColumn("cumulative_yards", _sum("yards_returned").over(window_cum))

##### 2ND PART PRODUCTION
##### Silver -> Gold transformation: Write transformations to HDFS/Hive
fact_df.write.mode("overwrite").insertInto("marc_gold_kick_return")
dim_player.write.mode("overwrite").insertInto("marc_gold_dim_player")
dim_team.write.mode("overwrite").insertInto("marc_gold_dim_team")
dim_year.write.mode("overwrite").insertInto("marc_gold_dim_year")

##### 2ND PART FOR LOCAL TESTING
# ## CSV's used for local testing of output files
# # Silver CSV
# silver_headers = [
#     "player_id","name","position","year","team","games_played",
#     "returns","yards_returned","yards_per_return","longest_return",
#     "returns_for_tds","returns_longer_20","returns_longer_40",
#     "fair_catches","fumbles","team_std","fair_catches_clean","returns_clean",
#     "yards_returned_clean","yards_per_return_clean","returns_longer_20_clean",
#     "returns_longer_40_clean","returns_for_tds_clean","fumbles_clean",
#     "longest_return_clean","long_return_flag","position_std"
# ]
#
# clean_df.toPandas().to_csv(
#     "./silver/silver_fact_kick_return_stats.csv",
#     index=False,
#     header=silver_headers
# )
#
# # Fact CSV
# fact_headers = [
#     "player_key","team_key","year_key","games_played","returns",
#     "yards_returned","yards_per_return","returns_longer_20","returns_longer_40",
#     "returns_for_tds","fumbles","success_rate_20plus","success_rate_40plus",
#     "turnover_risk","weighted_return_score","cumulative_yards"
# ]
#
# fact_df.toPandas().to_csv(
#     "./gold/gold_fact_kick_return_stats.csv",
#     index=False,
#     header=fact_headers
# )
#
# # Dim Player CSV
# dim_player_headers = [
#     "player_id","name","position_std","debut_year","last_year",
#     "total_yards","total_returns","career_span","rookie_numeric",
#     "avg_yards_per_year","avg_returns_per_year","player_key"
# ]
#
# dim_player.toPandas().to_csv(
#     "./gold/gold_dim_player.csv",
#     index=False,
#     header=dim_player_headers
# )
#
# # Dim Team CSV
# dim_team_headers = ["team_std","team_key"]
#
# dim_team.toPandas().to_csv(
#     "./gold/gold_dim_team.csv",
#     index=False,
#     header=dim_team_headers
# )
#
# # Dim Year CSV
# dim_year_headers = ["year","year_key"]
#
# dim_year.toPandas().to_csv(
#     "./gold/gold_dim_year.csv",
#     index=False,
#     header=dim_year_headers
# )
print("ETL completed: dim/fact tables written to HDFS.")

####### ETL VALIDATION TESTS FOR FOOTBALL KICK RETURN PIPELINE
# Logging dir setup
log_dir = "Marc_Individual_Task_ETL_Project/marc_ETL_task_output"
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "marc_ETL_task.txt")

def log(msg, file=None):
    """Prints live and writes to log file if provided."""
    print(msg, flush=True)
    if file:
        file.write(msg + "\n")
        file.flush()

bronze_df = staging_df
silver_df = clean_df

# Run test wrapper
def run_test(name, func, file=None):
    """Runs a test, logs PASS/FAIL using log(), never stops ETL."""
    try:
        func(file)
        log(f"✔ PASS: {name}", file)
    except Exception as e:
        # Capture and log the exception message (stack trace can be added if needed)
        log(f"❌ FAIL: {name} → {e}", file)

# Test functions to check for data integrity
def test_bronze_vs_silver_row_count(file=None):
    # Diagnostic line is optional
    log("Running Test: Bronze vs Silver row count", file)
    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    if bronze_count != silver_count:
        raise AssertionError(f"Silver changed row count! Bronze={bronze_count}, Silver={silver_count}")

def test_fact_vs_silver_row_count(file=None):
    log("Running Test: Fact vs Silver row count", file)
    fact_count = fact_df.count()
    silver_count = silver_df.count()
    if fact_count != silver_count:
        raise AssertionError(f"Fact table row count must equal Silver! Fact={fact_count}, Silver={silver_count}")

def test_foreign_keys(file=None):
    log("Running Test: Fact foreign keys -> checking missing refs", file)
    missing_player = fact_df.join(dim_player, "player_key", "left_anti").count()
    missing_team   = fact_df.join(dim_team, "team_key", "left_anti").count()
    missing_year   = fact_df.join(dim_year, "year_key", "left_anti").count()
    if missing_player != 0:
        raise AssertionError(f"Invalid player_key in Fact. Missing count={missing_player}")
    if missing_team != 0:
        raise AssertionError(f"Invalid team_key in Fact. Missing count={missing_team}")
    if missing_year != 0:
        raise AssertionError(f"Invalid year_key in Fact. Missing count={missing_year}")

def test_player_total_yards(file=None):
    log("Running Test: dim_player total_yards vs recomputed totals", file)
    recalc = silver_df.groupBy("player_id").agg(
        F.sum("yards_returned_clean").alias("expected_total_yards")
    )
    joined_check = dim_player.join(recalc, "player_id") \
        .where(F.col("total_yards") != F.col("expected_total_yards"))
    mismatch_count = joined_check.count()
    if mismatch_count != 0:
        # Optionally show sample rows for debugging
        sample = joined_check.limit(10).toPandas() if mismatch_count <= 100 else None
        extra = f"; sample_rows={len(sample)}" if sample is not None else ""
        raise AssertionError(f"total_yards mismatch in dim_player: {mismatch_count} mismatches{extra}")

def test_fact_aggregations(file=None):
    log("Running Test: Gold fact metric recalculations", file)
    fact_agg = fact_df.withColumn(
        "calc_success_rate_20plus",
        F.when(F.col("returns") > 0, F.col("returns_longer_20") / F.col("returns")).otherwise(0.0)
    ).withColumn(
        "calc_success_rate_40plus",
        F.when(F.col("returns") > 0, F.col("returns_longer_40") / F.col("returns")).otherwise(0.0)
    ).withColumn(
        "calc_turnover_risk",
        F.when(F.col("returns") > 0, F.col("fumbles") / F.col("returns")).otherwise(0.0)
    ).withColumn(
        "calc_weighted_return_score",
        (F.col("yards_returned") * F.col("returns_for_tds")).cast("double")
    ).withColumn(
        "calc_cumulative_yards",
        F.sum("yards_returned").over(window_cum)
    )

    mismatches = fact_agg.where(
        (F.abs(F.col("success_rate_20plus") - F.col("calc_success_rate_20plus")) > 1e-6) |
        (F.abs(F.col("success_rate_40plus") - F.col("calc_success_rate_40plus")) > 1e-6) |
        (F.abs(F.col("turnover_risk") - F.col("calc_turnover_risk")) > 1e-6) |
        (F.abs(F.col("weighted_return_score") - F.col("calc_weighted_return_score")) > 1e-6) |
        (F.abs(F.col("cumulative_yards") - F.col("calc_cumulative_yards")) > 1e-6)
    )

    mismatch_count = mismatches.count()
    if mismatch_count != 0:
        raise AssertionError(f"Gold fact metric mismatches found: {mismatch_count}")

def test_show_joined_sample(file=None):
    log("Running Test: Show joined Fact + Player + Team sample (no assert)", file)
    joined_vals = fact_df.join(dim_player, "player_key") \
        .join(dim_team, "team_key") \
        .join(dim_year, "year_key") \
        .select(
            "player_key", "name",
            "team_std", "year", "yards_returned",
            "returns", "success_rate_20plus"
        ).show(15, truncate=False)

# Put tests in a list (name, function)
tests = [
    ("Bronze vs Silver row count", test_bronze_vs_silver_row_count),
    ("Fact row count vs Silver", test_fact_vs_silver_row_count),
    ("Fact foreign keys", test_foreign_keys),
    ("dim_player total_yards", test_player_total_yards),
    ("Fact aggregations", test_fact_aggregations),
    ("Show joined Fact + Player + Team sample", test_show_joined_sample),
]

# Run tests
with open(log_path, "w", encoding="utf-8") as log_file:
    print("---------------ETL VALIDATION TESTS START---------------")
    for name, func in tests:
        run_test(name, func, log_file)
    print("---------------ETL VALIDATION TESTS END---------------")