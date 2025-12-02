import pytest
from pyspark.sql import SparkSession
import os
import sys

# Force Spark workers and driver to use Python3
PY3 = sys.executable  # the python running pytest (python3.6)

os.environ["PYSPARK_PYTHON"] = PY3
os.environ["PYSPARK_DRIVER_PYTHON"] = PY3

print("Using Python:", PY3)

from nfl_tranforms_all_for_test import (
    transform_defensive,
    transform_kicking,
    transform_passing,
    transform_players,
    transform_receiving,
    transform_return,
    transform_rushing
)


# -------------------------------------------------------------------
# SparkSession fixture
# -------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("pytest-nfl-all-transforms")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# -------------------------------------------------------------------
# DEFENSIVE TESTS
# -------------------------------------------------------------------
def test_defensive_basic(spark):
    df = spark.createDataFrame(
        [("abc/123", "Evans, Fred", "10", "200")],
        ["player_id", "name", "solo_tackles", "total_tackles"]
    )
    res = transform_defensive(df).first()
    assert res.player_id == 123


# -------------------------------------------------------------------
# KICKING TESTS
# -------------------------------------------------------------------
def test_kicking_basic(spark):
    df = spark.createDataFrame(
        [("k/77", "Smith, John", "25", "1975")],
        ["player_id", "name", "fgs_made", "year"]
    )
    res = transform_kicking(df).first()
    assert res.player_id == 77


# -------------------------------------------------------------------
# PASSING TESTS
# -------------------------------------------------------------------
def test_passing_filter(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "Doe, Tom", "50", "900"),     # filtered (below 1,000 yards)
            ("id/2", "Doe, Tim", "50", "2000"),    # should remain
        ],
        ["player_id", "name", "passes_attempted", "passing_yards"]
    )
    out = transform_passing(df)
    assert out.count() == 1


# -------------------------------------------------------------------
# PLAYERS TESTS
# -------------------------------------------------------------------
def test_players_birthday(spark):
    df = spark.createDataFrame(
        [("p/500", "Smith, Jane", "NY, NY", "01/02/1980")],
        ["player_id", "name", "birth_place", "birthday"]
    )
    res = transform_players(df).first()
    assert str(res.birthday) == "1980-01-02"


# -------------------------------------------------------------------
# RECEIVING TESTS
# -------------------------------------------------------------------
def test_receiving_yard_filter(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "Evans, Fred", "10", "800"),   # too low
            ("id/2", "Evans, Tim", "20", "1500"),   # remains
        ],
        ["player_id", "name", "receptions", "receiving_yards"]
    )
    out = transform_receiving(df)
    assert out.count() == 1


# -------------------------------------------------------------------
# RETURN TESTS
# -------------------------------------------------------------------
def test_return_td_filter(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "Smith, Joe", "1", "0"),   # rejects punt return td
            ("id/2", "Smith, Tim", "1", "1"),   # valid
        ],
        ["player_id", "name", "kick_returns_for_tds", "punt_returns_for_tds"]
    )
    out = transform_return(df)
    assert out.count() == 1


# -------------------------------------------------------------------
# RUSHING TESTS
# -------------------------------------------------------------------
def test_rushing_yard_filter(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "Doe, A", "20", "900"),   # below threshold
            ("id/2", "Doe, B", "30", "2000"),  # valid
        ],
        ["player_id", "name", "rushing_attempts", "rushing_yards"]
    )
    out = transform_rushing(df)
    assert out.count() == 1