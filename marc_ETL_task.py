"""
Author: Marc Hanna
Description: ETL script to process kick return stats CSV into Star Schema (Raw → Silver → Gold) using PySpark.
"""
##### Initialize Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, coalesce, when,
    sum as _sum, min as _min, max as _max, round as _round
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("KickReturnETL").getOrCreate()


##### Read CSV from HDFS (clean all data next)
staging_df = spark.read.csv(
    "hdfs:///tmp/DE011025/marc/sqoop/career_stats_kick_return/part-m-00000",
    header=False,
    inferSchema=True
)
staging_df = staging_df.toDF(
    "player_id","last_name","first_name","middle_initial","year","team",
    "games_played","fair_catches","returns","yards_returned","yards_per_return",
    "returns_longer_20","returns_longer_40","returns_for_tds","fumbles","longest_return","position"
)

##### Write raw staging table to the correct raw location: raw-> Bronze
staging_df.write.mode("overwrite").parquet(
    "hdfs:///tmp/de011025/marc/raw/staging_career_stats_kick_return"
)

staging_df = staging_df.toDF(
    "player_id","last_name","first_name","middle_initial","year","team",
    "games_played","fair_catches","returns","yards_returned","yards_per_return",
    "returns_longer_20","returns_longer_40","returns_for_tds","fumbles","longest_return","position"
)


##### Clean/Rename/Standardize (NOT counted as TRANSFORMATIONs)
clean_df = staging_df.select(
    col("player_id"),
    col("last_name"),
    col("first_name"),
    col("middle_initial"),
    col("year"),
    # standardize team capitalization
    upper(trim(col("team"))).alias("team_std"),
    col("games_played"),
    coalesce(col("fair_catches"), 0).alias("fair_catches_clean"),
    coalesce(col("returns"), 0).alias("returns_clean"),
    coalesce(col("yards_returned"), 0).alias("yards_returned_clean"),
    coalesce(col("yards_per_return"), 0).alias("yards_per_return_clean"),
    coalesce(col("returns_longer_20"), 0).alias("returns_longer_20_clean"),
    coalesce(col("returns_longer_40"), 0).alias("returns_longer_40_clean"),
    coalesce(col("returns_for_tds"), 0).alias("returns_for_tds_clean"),
    coalesce(col("fumbles"), 0).alias("fumbles_clean"),
    coalesce(col("longest_return"), 0).alias("longest_return_clean"),
    # flag for long return metric (counts as enrichment of data)
    when(coalesce(col("longest_return"),0) > 40, 1).otherwise(0).alias("long_return_flag"),
    upper(trim(col("position"))).alias("position_std")
)

##### Build dim_player
player_agg = clean_df.groupBy("player_id","first_name","last_name","position_std") \
    .agg(
        # compute min/max years and totals (used to derive TRANSFORMATION 1)
        _min("year").alias("debut_year"),
        _max("year").alias("last_year"),
        _sum("yards_returned_clean").alias("total_yards"),
        _sum("returns_clean").alias("total_returns")
    )

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
dim_team = clean_df.select("team_std").withColumn("team_key", row_number().over(window_team))


##### Build dim_year
dim_year = clean_df.select("year").distinct() \
    .withColumn("year_key", row_number().over(Window.orderBy("year")))

# Cleaned data: Bronze -> Silver transformation
clean_df.write.mode("overwrite").parquet("hdfs:///tmp/de011025/marc/Silver/silver_fact_kick_return_stats")
dim_player.write.mode("overwrite").parquet("hdfs:///tmp/de011025/marc/Silver/silver_dim_player")
dim_team.write.mode("overwrite").parquet("hdfs:///tmp/de011025/marc/Silver/silver_dim_team")
dim_year.write.mode("overwrite").parquet("hdfs:///tmp/de011025/marc/Silver/silver_dim_year")


##### Create fact_kick_return_stats and compute business-value metrics
# join staging to dimension keys
s = clean_df.alias("s")
p = dim_player.select("player_id","player_key").alias("p")
t = dim_team.select("team_std","team_key").alias("t")
y = dim_year.select("year","year_key").alias("y")
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


##### Write tables to HDFS: Silver -> Gold transformation
fact_df.write.mode("overwrite").parquet("hdfs:///tmp/DE011025/marc/Gold/gold_fact_kick_return_stats")


print("ETL completed: dim/fact tables written to HDFS.")
