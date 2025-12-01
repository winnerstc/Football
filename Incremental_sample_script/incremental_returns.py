from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, split, size, when, max as spark_max
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("ReturnsIncremental").getOrCreate()

raw_path = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/DE011025/Joe/raw/nfl_returns/"
silver_path = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/DE011025/Joe/silver/nfl_returns_output/"

# Watermark
try:
    silver_df = spark.read.parquet(silver_path)
    watermark = silver_df.agg(spark_max("player_id")).collect()[0][0] or 0
except:
    watermark = 0

df = spark.read.option("header", True).parquet(raw_path)

df = df.withColumn("player_id", regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType()))

df = df.filter(col("player_id") > watermark)

if df.count() == 0:
    spark.stop()
    exit()

# Replace --
dash_cols = [
 "kick_returns","yards_kick_returned","kick_returns_for_tds",
 "punt_returns","yards_punt_returned","punt_returns_for_tds"
]
df = df.na.replace({"--":"0"}, subset=dash_cols)

# Name split
df = df.withColumn("name_split", split(col("name"), ", "))
df = df.withColumn("last_name", col("name_split")[0])
df = df.withColumn("first_name",
                   when(size(col("name_split"))>1, col("name_split")[1]))

# Remove bad year < 1970
df = df.filter(col("year") >= 1970)

# Cast numeric
for c in dash_cols + ["year"]:
    df = df.withColumn(c, col(c).cast(IntegerType()))

df = df.drop("name_split","name").dropDuplicates()

df.write.mode("append").parquet(silver_path)
spark.stop()
