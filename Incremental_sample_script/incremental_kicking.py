from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, max as spark_max
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder.appName("KickingIncremental").getOrCreate()

raw_path = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/DE011025/Joe/raw/nfl_kicking/"
silver_path = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/DE011025/Joe/silver/nfl_kicking_output/"

# 1. High Watermark
try:
    silver_df = spark.read.parquet(silver_path)
    watermark = silver_df.agg(spark_max("player_id")).collect()[0][0] or 0
except:
    watermark = 0

# 2. Read raw
df = spark.read.option("header", True).parquet(raw_path)

# Clean ID
df = df.withColumn("player_id", regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType()))

# Filter only new
df = df.filter(col("player_id") > watermark)

if df.count() == 0:
    spark.stop()
    exit()

# Replace dashes with 0
dash_cols = ["fgs_made","fgs_attempted","fg_percentage"]
replace_dict = {"--": "0"}
df = df.na.replace(replace_dict, subset=dash_cols)

# Cast numeric
int_cols = ["games_played","longest_fg_made","fgs_made","fgs_attempted"]
float_cols = ["fg_percentage"]

for c in int_cols:
    df = df.withColumn(c, col(c).cast(IntegerType()))

for c in float_cols:
    df = df.withColumn(c, col(c).cast(FloatType()))

df = df.dropDuplicates()

df.write.mode("append").parquet(silver_path)
spark.stop()
