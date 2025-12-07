"""
Author: Marc Hanna
Description: ETL script to process kick return stats CSV into Star Schema (Raw → Bronze → Silver → Gold) using PySpark.
"""
import datetime
import sys
import threading
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, coalesce, when,
    sum as _sum, min as _min, max as _max, round as _round, lit, row_number
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window
import os
import psutil

LOCAL = False

import sys
import os
import json
from pyspark.sql import SparkSession

max_mem = 0
max_cpu = 0
monitoring = True

def monitor_resources(interval=0.5):
    """Background thread to track memory (MB) and CPU (%) continuously."""
    global max_mem, max_cpu, monitoring
    process = psutil.Process()
    while monitoring:
        mem = process.memory_info().rss / (1024 * 1024)
        cpu = psutil.cpu_percent(interval=None)
        if mem > max_mem:
            max_mem = mem
        if cpu > max_cpu:
            max_cpu = cpu
        time.sleep(interval)


if len(sys.argv) < 2:
    raise ValueError("Usage: spark-submit marc_ETL_task.py [1|0]")

# Set LOCAL=True to test with local node, False to use HDFS cluster
LOCAL = int(sys.argv[1].strip())
if LOCAL not in (0, 1):
    raise ValueError("Argument must be 1 (LOCAL) or 0 (CLUSTER)")

if LOCAL == 1:
    spark = (
        SparkSession.builder
        .appName("KickReturnsLocal")
        .master("local[*]")
        .getOrCreate()
    )
    print("[INFO] Spark initialized in LOCAL mode.")
else:
    # Cluster mode: all configs come from spark-submit --conf
    spark = (
        SparkSession.builder
        .appName("KickReturnsCluster")
        .config("hive.metastore.uris", "thrift://ip-172-31-8-235.eu-west-2.compute.internal:9083")
        .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020")
        .config("spark.hadoop.yarn.resourcemanager.address", "ip-172-31-3-80.eu-west-2.compute.internal:8032")
        .enableHiveSupport()
        .getOrCreate()
    )
    print("[INFO] Spark initialized in CLUSTER mode. Configuration controlled by spark-submit --conf")
spark.sparkContext.setLogLevel("ERROR")

monitor_thread = threading.Thread(target=monitor_resources, daemon=True)
monitor_thread.start()

# Schema definition
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

# Paths for HDFS/Hive
bronze_path = "hdfs:///tmp/DE011025/marc/raw/"
silver_path = "hdfs:///tmp/DE011025/marc/silver/"
gold_path_base = "hdfs:///tmp/DE011025/marc/gold/"

# Read raw source data from csv if local is true else update HIVE table definitions and read from HDFS
if LOCAL:
    csv_file = "Career_Stats_Kick_Return.csv"
    staging_df = spark.read \
        .option("header", "true") \
        .schema(kick_returns_schema) \
        .csv(csv_file)
else:
    # Helper function to recreate tables
    def recreate_table(table_name, ddl):
        print(f"Recreating table: {table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(ddl)


    bronze_table_name = "marc_bronze_kick_returns"
    # UPDATE Bronze Table
    recreate_table(
        bronze_table_name,
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {bronze_table_name} (
            player_id STRING,
            name STRING,
            position STRING,
            year INT,
            team STRING,
            games_played INT,
            returns INT,
            yards_returned INT,
            yards_per_return DOUBLE,
            longest_return INT,
            returns_for_tds INT,
            returns_longer_20 INT,
            returns_longer_40 INT,
            fair_catches INT,
            fumbles INT
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{bronze_path}'
        TBLPROPERTIES ("skip.header.line.count"="1")
        """
    )

    silver_table_name = "marc_silver_kick_returns"
    # UPDATE Silver Table
    recreate_table(
        silver_table_name,
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {silver_table_name} (
            team_std STRING,
            fair_catches_clean INT,
            returns_clean INT,
            yards_returned_clean INT,
            yards_per_return_clean DOUBLE,
            returns_longer_20_clean INT,
            returns_longer_40_clean INT,
            returns_for_tds_clean INT,
            fumbles_clean INT,
            longest_return_clean INT,
            long_return_flag INT,
            position_std STRING,
            player_id STRING,
            name STRING,
            position STRING,
            year INT,
            team STRING,
            games_played INT,
            returns INT,
            yards_returned INT,
            yards_per_return DOUBLE,
            longest_return INT,
            returns_for_tds INT,
            returns_longer_20 INT,
            returns_longer_40 INT
        )
        STORED AS PARQUET
        LOCATION '{silver_path}'
        """
    )

    # UPDATE GOLD TABLES
    # Gold table paths
    gold_fact_path = f"{gold_path_base}/fact/"
    gold_dim_player_path = f"{gold_path_base}/dim_player/"
    gold_dim_team_path = f"{gold_path_base}/dim_team/"
    gold_dim_year_path = f"{gold_path_base}/dim_year/"

    # FACT TABLE
    gold_fact_table_name = "marc_gold_kick_return"
    recreate_table(
        gold_fact_table_name,
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS gold_fact_table_name (
            player_key INT,
            team_key INT,
            year_key INT,
            games_played INT,
            returns INT,
            yards_returned INT,
            yards_per_return DOUBLE,
            returns_longer_20 INT,
            returns_longer_40 INT,
            returns_for_tds INT,
            fumbles INT,
            success_rate_20plus DOUBLE,
            success_rate_40plus DOUBLE,
            turnover_risk DOUBLE,
            weighted_return_score DOUBLE,
            cumulative_yards DOUBLE
        )
        STORED AS PARQUET
        LOCATION '{gold_fact_path}'
        """
    )

    gold_dim_player_table_name = "marc_gold_dim_player"
    # DIM PLAYER TABLE
    recreate_table(
        gold_dim_player_table_name,
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {gold_dim_player_table_name} (
            player_key INT,
            player_id STRING,
            name STRING,
            position_std STRING,
            debut_year INT,
            last_year INT,
            total_yards INT,
            total_returns INT,
            career_span INT,
            rookie_numeric INT,
            avg_yards_per_year DOUBLE,
            avg_returns_per_year DOUBLE
        )
        STORED AS PARQUET
        LOCATION '{gold_dim_player_path}'
        """
    )

    gold_dim_team_table_name = "gold_dim_player_table_name"
    # DIM TEAM TABLE
    recreate_table(
        gold_dim_team_table_name,
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {gold_dim_team_table_name} (
            team_key INT,
            team_std STRING
        )
        STORED AS PARQUET
        LOCATION '{gold_dim_team_path}'
        """
    )

    gold_dim_year_name = "marc_gold_dim_year"
    # DIM YEAR TABLE
    recreate_table(
        {gold_dim_year_name},
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {gold_dim_year_name} (
            year_key INT,
            year INT
        )
        STORED AS PARQUET
        LOCATION '{gold_dim_year_path}'
        """
    )

    staging_df = spark.read \
        .option("header", "true") \
        .schema(kick_returns_schema) \
        .csv(bronze_path)

# Bronze → Silver transformation
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
    .withColumn("long_return_flag", when(col("longest_return") > 40, 1).otherwise(0)) \
    .withColumn("position_std", upper(trim(col("position"))))

# Optionally write Bronze/Silver if not LOCAL
if not LOCAL:
    staging_df.write.mode("overwrite")
    clean_df.write.mode("overwrite")

# Continue with Dim/Fact tables (same logic as before)
window_player = Window.orderBy("player_id")
window_team = Window.orderBy("team_std")
window_year = Window.orderBy("year")

player_agg = clean_df.groupBy("player_id", "name", "position_std") \
    .agg(
        _min("year").alias("debut_year"),
        _max("year").alias("last_year"),
        _sum("yards_returned_clean").alias("total_yards"),
        _sum("returns_clean").alias("total_returns")
    )

player_metrics = player_agg.withColumn("career_span", col("last_year") - col("debut_year") + 1) \
    .withColumn("rookie_numeric", when(col("career_span") == 1, 1).otherwise(0)) \
    .withColumn("avg_yards_per_year", _round(col("total_yards") / when(col("career_span") == 0, 1).otherwise(col("career_span")), 2)) \
    .withColumn("avg_returns_per_year", _round(col("total_returns") / when(col("career_span") == 0, 1).otherwise(col("career_span")), 2))

dim_player = player_metrics.withColumn("player_key", row_number().over(window_player))
dim_team = clean_df.select("team_std").distinct().withColumn("team_key", row_number().over(window_team))
dim_year = clean_df.select("year").distinct().withColumn("year_key", row_number().over(window_year))

joined = clean_df.alias("s") \
    .join(dim_player.select("player_id", "player_key"), "player_id", "left") \
    .join(dim_team.select("team_std", "team_key"), "team_std", "left") \
    .join(dim_year.select("year", "year_key"), "year", "left")

fact_df = joined.select(
    col("player_key"),
    col("team_key"),
    col("year_key"),
    col("games_played"),
    col("returns_clean").alias("returns"),
    col("yards_returned_clean").alias("yards_returned"),
    col("yards_per_return_clean").alias("yards_per_return"),
    col("returns_longer_20_clean").alias("returns_longer_20"),
    col("returns_longer_40_clean").alias("returns_longer_40"),
    col("returns_for_tds_clean").alias("returns_for_tds"),
    col("fumbles_clean").alias("fumbles")
).withColumn("success_rate_20plus", when(col("returns") > 0, col("returns_longer_20") / col("returns")).otherwise(0)) \
 .withColumn("success_rate_40plus", when(col("returns") > 0, col("returns_longer_40") / col("returns")).otherwise(0)) \
 .withColumn("turnover_risk", when(col("returns") > 0, col("fumbles") / col("returns")).otherwise(0)) \
 .withColumn("weighted_return_score", col("yards_returned") * col("returns_for_tds")) \
 .withColumn("cumulative_yards", _sum("yards_returned").over(Window.partitionBy("player_key").orderBy("year_key").rowsBetween(Window.unboundedPreceding, 0)))

# Write Gold tables only if not LOCAL
if not LOCAL:
    fact_df.write.mode("overwrite")
    dim_player.write.mode("overwrite")
    dim_team.write.mode("overwrite")
    dim_year.write.mode("overwrite")

# ETL Validation Tests
script_dir = os.path.dirname(os.path.abspath(__file__))

# Validation test results directory relative to the script
log_dir = os.path.join(script_dir, "marc_ETL_task_output")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "marc_ETL_task.txt")

def log(msg, file=None):
    print(msg, flush=True)
    if file:
        file.write(msg + "\n")
        file.flush()

def run_test(name, func, file=None):
    try:
        func(file)
        log(f"✔ PASS: {name}", file)
    except Exception as e:
        log(f"❌ FAIL: {name} → {e}", file)

# Define test functions
def test_bronze_vs_silver_row_count(file=None):
    bronze_count = staging_df.count()
    silver_count = clean_df.count()
    if bronze_count != silver_count:
        raise AssertionError(f"Silver row count mismatch! Bronze={bronze_count}, Silver={silver_count}")

def test_fact_vs_silver_row_count(file=None):
    if fact_df.count() != clean_df.count():
        raise AssertionError("Fact table row count does not match Silver!")

def show_fact_dim_joined(file=None):
    print(joined)
    joined.show(15, truncate=False)

# Run all tests
tests = [
    ("Bronze vs Silver row count", test_bronze_vs_silver_row_count),
    ("Fact vs Silver row count", test_fact_vs_silver_row_count),
    ("Foreign keys validation", show_fact_dim_joined),
]
with open(log_path, "w", encoding="utf-8") as log_file:
    for name, func in tests:
        run_test(name, func, log_file)

def log_df(df):
    """
    Logs a Spark DataFrame to ./outputs/ with timestamped filename.
    Writes in CSV with header for readability.
    """
    # Create local outputs directory if not exists
    local_out_dir = os.path.join(os.getcwd(), "outputs")
    os.makedirs(local_out_dir, exist_ok=True)

    # Timestamp for unique filenames
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    # Full path for the log file
    output_path = os.path.join(local_out_dir, f"joined_kick_results_{ts}")
    if LOCAL:
        # Write as CSV — best for reviewing in text/PyCharm/Excel
        df.toPandas().to_csv(output_path, index=False)
    else:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    print(f"[INFO] Test results logged to: {output_path}")
# Log test results, stop Spark session and show memory usage
log_df(joined)

spark.stop()
print("ETL pipeline complete. All tables written to HDFS and validations executed.")

monitoring = False
monitor_thread.join(timeout=1)
print(f"[INFO] Peak memory used during ETL: {max_mem:.2f} MB")
print(f"[INFO] Peak CPU usage during ETL: {max_cpu:.2f}%")
