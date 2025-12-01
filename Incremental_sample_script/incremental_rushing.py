from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, max as spark_max
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("RushingIncremental").getOrCreate()

raw_path = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/DE011025/Joe/raw/nfl_rushing/"
silver_path = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/DE011025/Joe/silver/nfl_rushing_output/"

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

for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, trim(col(c)))

num_cols = ["attempts","yards","tds"]
for c in num_cols:
    df = df.withColumn(c, col(c).cast(IntegerType()))

df = df.dropDuplicates()
df.write.mode("append").parquet(silver_path)
spark.stop()
