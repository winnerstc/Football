import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType

# Ensure Spark Worker = Spark Driver Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from nfl_tranforms_all_for_test import (
    transform_defensive,
    transform_kicking,
    transform_passing,
    transform_players,
    transform_receiving,
    transform_return,
    transform_rushing,
)


# ------------------------------------------------------------------------------
# Spark Session Fixture
# ------------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("nfl-transform-tests")
        .getOrCreate()
    )


# ==============================================================================
# 1) DEFENSIVE TESTS
# ==============================================================================
def test_defensive_basic(spark):
    df = spark.createDataFrame(
        [("abc/123", "Evans, Fred", "10", "200")],
        ["player_id", "name", "solo_tackles", "total_tackles"],
    )
    r = transform_defensive(df).first()
    assert r.player_id == 123
    assert r.first_name == "Fred"
    assert r.last_name == "Evans"
    assert r.solo_tackles == 10


def test_defensive_filter_total_tackles(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "A, B", "5", "50"),    # DROP
            ("id/2", "C, D", "10", "150"),  # KEEP
        ],
        ["player_id", "name", "solo_tackles", "total_tackles"],
    )
    out = transform_defensive(df).collect()
    assert len(out) == 1
    assert out[0].player_id == 2


def test_defensive_missing_optional_columns(spark):
    df = spark.createDataFrame(
        [("123", "Doe, John")],
        ["player_id", "name"],
    )
    out = transform_defensive(df).first()
    assert out.first_name == "John"
    assert out.last_name == "Doe"


def test_defensive_handles_empty_strings(spark):
    df = spark.createDataFrame(
        [("id/10", "Smith, Joe", "", "")],
        ["player_id", "name", "solo_tackles", "total_tackles"],
    )
    out = transform_defensive(df).collect()
    assert len(out) == 0  # fails total_tackles >= 100


# ==============================================================================
# 2) KICKING TESTS
# ==============================================================================
def test_kicking_basic(spark):
    df = spark.createDataFrame(
        [("id/55", "Tucker, Justin", "25", "50")],
        ["player_id", "name", "fgs_made", "longest_fg_made"],
    )
    r = transform_kicking(df).first()
    assert r.player_id == 55
    assert r.fgs_made == 25


def test_kicking_filter_fgs_made(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "A, B", "10", "30"),   # DROP (<20)
            ("id/2", "C, D", "22", "40"),   # KEEP
        ],
        ["player_id", "name", "fgs_made", "longest_fg_made"],
    )
    out = transform_kicking(df).collect()
    assert len(out) == 1
    assert out[0].player_id == 2


def test_kicking_optional_columns(spark):
    df = spark.createDataFrame(
        [("id/100", "Doe, Jane", "30")],
        ["player_id", "name", "fgs_made"],
    )
    out = transform_kicking(df).first()
    assert out.fgs_made == 30


# ==============================================================================
# 3) PASSING TESTS
# ==============================================================================
def test_passing_basic(spark):
    df = spark.createDataFrame(
        [("id/10", "Brady, Tom", "350", "2000")],
        ["player_id", "name", "passes_attempted", "passing_yards"],
    )
    r = transform_passing(df).first()
    assert r.passing_yards == 2000


def test_passing_filter_yards(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "Low, Guy", "100", "500"),   # DROP
            ("id/2", "High, Guy", "300", "1500"), # KEEP
        ],
        ["player_id", "name", "passes_attempted", "passing_yards"],
    )
    out = transform_passing(df).collect()
    assert len(out) == 1


def test_passing_missing_columns(spark):
    df = spark.createDataFrame(
        [("id/77", "Doe, Jim")],
        ["player_id", "name"],
    )
    out = transform_passing(df).first()
    assert out.first_name == "Jim"


# ==============================================================================
# 4) PLAYER TESTS
# ==============================================================================
def test_players_birthday_valid(spark):
    df = spark.createDataFrame(
        [("id/5", "Smith, Alex", "04/15/1982", "Texas, USA")],
        ["player_id", "name", "birthday", "birth_place"],
    )
    r = transform_players(df).first()
    assert str(r.birthday).startswith("1982")


def test_players_birthday_invalid(spark):
    df = spark.createDataFrame(
        [("id/10", "Doe, John", "not-a-date", "X, Y")],
        ["player_id", "name", "birthday", "birth_place"],
    )
    out = transform_players(df).collect()
    assert len(out) == 0  # invalid birthday filtered out


def test_players_birthplace_split(spark):
    df = spark.createDataFrame(
        [("id/20", "Brown, Mike", "01/01/1990", "Chicago, IL")],
        ["player_id", "name", "birthday", "birth_place"],
    )
    r = transform_players(df).first()
    assert r.birth_city == "Chicago"
    assert r.birth_state == "IL"


# ==============================================================================
# 5) RECEIVING TESTS
# ==============================================================================
def test_receiving_basic(spark):
    df = spark.createDataFrame(
        [("id/1", "Jones, Julio", "90", "1500")],
        ["player_id", "name", "receptions", "receiving_yards"],
    )
    r = transform_receiving(df).first()
    assert r.receiving_yards == 1500


def test_receiving_filter_yards(spark):
    df = spark.createDataFrame(
        [
            ("id/10", "Small, Guy", "50", "200"),    # DROP
            ("id/11", "Big, Guy", "90", "1200"),     # KEEP
        ],
        ["player_id", "name", "receptions", "receiving_yards"],
    )
    out = transform_receiving(df).collect()
    assert len(out) == 1


def test_receiving_longest_T_clean(spark):
    df = spark.createDataFrame(
        [("id/9", "Fast, Runner", "30", "1100", "85T")],
        ["player_id", "name", "receptions", "receiving_yards", "longest_reception"],
    )
    r = transform_receiving(df).first()
    assert r.longest_reception == 85


# ==============================================================================
# 6) RETURN TESTS
# ==============================================================================
def test_return_basic(spark):
    df = spark.createDataFrame(
        [("id/7", "Hester, Devin", "5")],
        ["player_id", "name", "kick_returns_for_tds"],
    )
    r = transform_return(df).first()
    assert r.kick_returns_for_tds == 5


def test_return_filter_tds(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "Guy, Zero", "0"),   # DROP
            ("id/2", "Guy, One", "1"),    # KEEP
        ],
        ["player_id", "name", "kick_returns_for_tds"],
    )
    out = transform_return(df).collect()
    assert len(out) == 1


# ==============================================================================
# 7) RUSHING TESTS
# ==============================================================================
def test_rushing_basic(spark):
    df = spark.createDataFrame(
        [("id/20", "Henry, Derrick", "300", "2027")],
        ["player_id", "name", "rushing_attempts", "rushing_yards"],
    )
    r = transform_rushing(df).first()
    assert r.rushing_yards == 2027


def test_rushing_filter_yards(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "Small, Runner", "200", "500"),   # DROP
            ("id/2", "Big, Runner", "350", "1300"),    # KEEP
        ],
        ["player_id", "name", "rushing_attempts", "rushing_yards"],
    )
    out = transform_rushing(df).collect()
    assert len(out) == 1


def test_rushing_longest_run_T_clean(spark):
    df = spark.createDataFrame(
        [("id/8", "Speedy, Joe", "200", "1500", "70T")],
        ["player_id", "name", "rushing_attempts", "rushing_yards", "longest_rushing_run"],
    )
    r = transform_rushing(df).first()
    assert r.longest_rushing_run == 70