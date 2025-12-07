import os
import sys
import datetime
import pytest
from pyspark.sql import SparkSession


# -------------------------------
# Spark Session Fixture
# -------------------------------
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("hive-table-tests")
        .enableHiveSupport()  # enable Hive
        .master("local[*]")
        .getOrCreate()
    )

# 
# Hive Table Fixtures
@pytest.fixture(scope="session")
def bronze_df(spark):
    return spark.sql("SELECT * FROM marc_bronze_kick_returns")

@pytest.fixture(scope="session")
def silver_df(spark):
    return spark.sql("SELECT * FROM marc_silver_kick_returns")

@pytest.fixture(scope="session")
def fact_df(spark):
    return spark.sql("SELECT * FROM marc_gold_kick_return")

@pytest.fixture(scope="session")
def dim_player_df(spark):
    return spark.sql("SELECT * FROM marc_gold_dim_player")

@pytest.fixture(scope="session")
def dim_team_df(spark):
    return spark.sql("SELECT * FROM marc_gold_dim_team")

@pytest.fixture(scope="session")
def dim_year_df(spark):
    return spark.sql("SELECT * FROM marc_gold_dim_year")

@pytest.fixture(scope="session")
def fact_dim_joined(spark):
    return spark.sql("""
        SELECT f.*, p.*, t.*, y.*
        FROM marc_gold_kick_return f
        LEFT JOIN marc_gold_dim_player p ON f.player_key = p.player_key
        LEFT JOIN marc_gold_dim_team t ON f.team_key = t.team_key
        LEFT JOIN marc_gold_dim_year y ON f.year_key = y.year_key
    """)

# -------------------------------
# Tests
# -------------------------------
def test_bronze_vs_silver_row_count(bronze_df, silver_df):
    """Check Bronze and Silver row counts match"""
    assert bronze_df.count() == silver_df.count(), \
        f"Silver row count mismatch! Bronze={bronze_df.count()}, Silver={silver_df.count()}"

def test_fact_vs_silver_row_count(fact_df, silver_df):
    """Check Fact table row count matches Silver"""
    assert fact_df.count() == silver_df.count(), \
        f"Fact row count does not match Silver! Fact={fact_df.count()}, Silver={silver_df.count()}"

def test_fact_dim_joined_not_empty(fact_dim_joined):
    """Check joined Fact + Dimensions is not empty"""
    count = fact_dim_joined.count()
    assert count > 0, "Joined Fact/Dim table is empty"
    fact_dim_joined.show(10, truncate=False)

def test_dim_player_has_keys(dim_player_df):
    """Check player dimension has player_key"""
    keys = dim_player_df.select("player_key").dropna().count()
    assert keys > 0, "Player dimension table has no keys"

def test_dim_team_has_keys(dim_team_df):
    """Check team dimension has team_key"""
    keys = dim_team_df.select("team_key").dropna().count()
    assert keys > 0, "Team dimension table has no keys"

def test_dim_year_has_keys(dim_year_df):
    """Check year dimension has year_key"""
    keys = dim_year_df.select("year_key").dropna().count()
    assert keys > 0, "Year dimension table has no keys"

# # Optional: logging utility
# def log_df(df, name="output", local=True):
#     """
#     Logs a Spark DataFrame to ./outputs/ with timestamped filename.
#     """
#     out_dir = os.path.join(os.getcwd(), "outputs")
#     os.makedirs(out_dir, exist_ok=True)
#
#     ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#     output_path = os.path.join(out_dir, f"{name}_{ts}.csv")
#
#     if local:
#         df.toPandas().to_csv(output_path, index=False)
#     else:
#         df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
#
#     print(f"[INFO] DataFrame logged to: {output_path}")