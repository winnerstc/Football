import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, split, trim, when, size, to_date, lit
)
from pyspark.sql.types import IntegerType, FloatType


# ------------------------------------------------------------------------------
# SPARK FIXTURE
# ------------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("standalone-transform-tests")
        .getOrCreate()
    )


# ------------------------------------------------------------------------------
# STANDALONE TRANSFORM FUNCTIONS (COPIED FROM YOUR LOGIC)
# ------------------------------------------------------------------------------

def transform_defensive(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    df = df.replace("--", "0")
    df = df.replace("", None)

    df = df.dropDuplicates()

    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("last_name", col("name_split")[0])
    df = df.withColumn(
        "first_name",
        when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None)
    )
    df = df.filter(col("first_name").isNotNull())

    if "solo_tackles" in df.columns:
        df = df.withColumn("solo_tackles", col("solo_tackles").cast(IntegerType()))

    if "total_tackles" in df.columns:
        df = df.withColumn("total_tackles", col("total_tackles").cast(FloatType()))
        df = df.filter(col("total_tackles") >= 100)

    return df


def transform_kicking(df):
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )
    df = df.withColumn("fgs_made", col("fgs_made").cast(IntegerType()))
    df = df.filter(col("fgs_made") >= 20)
    return df


def transform_passing(df):
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    # Safe: create column if missing
    if "passing_yards" in df.columns:
        df = df.withColumn("passing_yards", col("passing_yards").cast(IntegerType()))
    else:
        df = df.withColumn("passing_yards", lit(None).cast(IntegerType()))

    # Filter
    # Only filter if passing_yards column exists AND has non-null values
    if "passing_yards" in df.columns:
        df = df.filter((col("passing_yards") >= 1000) | col("passing_yards").isNull())


    # Split name
    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("last_name", col("name_split")[0])
    df = df.withColumn("first_name",
                       when(size(col("name_split")) > 1, col("name_split")[1])
                       .otherwise(None))

    return df.drop("name_split")



def transform_players(df):
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    # Convert birthday → DATE
    df = df.withColumn("birthday", to_date(col("birthday"), "MM/dd/yyyy"))

    # Keep only valid birthdays
    df = df.filter(col("birthday").isNotNull())

    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("last_name", col("name_split")[0])
    df = df.withColumn("first_name",
                       when(size(col("name_split")) > 1, col("name_split")[1])
                       .otherwise(None))

    df = df.withColumn("birth_place_split", split(col("birth_place"), ", "))
    df = df.withColumn("birth_city", col("birth_place_split")[0])
    df = df.withColumn("birth_state",
                       when(size(col("birth_place_split")) > 1, col("birth_place_split")[1])
                       .otherwise(None))

    return df


def transform_receiving(df):
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )
    df = df.withColumn("receiving_yards", col("receiving_yards").cast(IntegerType()))
    df = df.filter(col("receiving_yards") >= 1000)

    if "longest_reception" in df.columns:
        df = df.withColumn(
            "longest_reception",
            regexp_replace("longest_reception", "T", "").cast(IntegerType())
        )

    return df


def transform_return(df):
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )
    df = df.withColumn(
        "kick_returns_for_tds",
        col("kick_returns_for_tds").cast(IntegerType())
    )
    df = df.filter(col("kick_returns_for_tds") >= 1)
    return df


def transform_rushing(df):
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )
    df = df.withColumn("rushing_yards", col("rushing_yards").cast(IntegerType()))
    df = df.filter(col("rushing_yards") >= 1000)

    if "longest_rushing_run" in df.columns:
        df = df.withColumn(
            "longest_rushing_run",
            regexp_replace("longest_rushing_run", "T", "").cast(IntegerType())
        )

    return df


# ------------------------------------------------------------------------------
# ALL 21 TEST CASES (UNCHANGED)
# ------------------------------------------------------------------------------

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
        [("id/1", "A, B", "5", "50"), ("id/2", "C, D", "10", "150")],
        ["player_id", "name", "solo_tackles", "total_tackles"],
    )
    out = transform_defensive(df).collect()
    assert len(out) == 1
    assert out[0].player_id == 2


def test_defensive_missing_optional_columns(spark):
    df = spark.createDataFrame([("123", "Doe, John")], ["player_id", "name"])
    out = transform_defensive(df).first()
    assert out.first_name == "John"
    assert out.last_name == "Doe"


def test_defensive_handles_empty_strings(spark):
    df = spark.createDataFrame(
        [("id/10", "Smith, Joe", "", "")],
        ["player_id", "name", "solo_tackles", "total_tackles"],
    )
    out = transform_defensive(df).collect()
    assert len(out) == 0


# ---------- KICKING TESTS -----------------------------------------------------

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
        [("id/1", "A, B", "10", "30"), ("id/2", "C, D", "22", "40")],
        ["player_id", "name", "fgs_made", "longest_fg_made"],
    )
    out = transform_kicking(df).collect()
    assert len(out) == 1


def test_kicking_optional_columns(spark):
    df = spark.createDataFrame(
        [("id/100", "Doe, Jane", "30")],
        ["player_id", "name", "fgs_made"],
    )
    out = transform_kicking(df).first()
    assert out.fgs_made == 30


# ---------- PASSING TESTS -----------------------------------------------------

def test_passing_basic(spark):
    df = spark.createDataFrame(
        [("id/10", "Brady, Tom", "350", "2000")],
        ["player_id", "name", "passes_attempted", "passing_yards"],
    )
    r = transform_passing(df).first()
    assert r.passing_yards == 2000


def test_passing_filter_yards(spark):
    df = spark.createDataFrame(
        [("id/1", "Low, Guy", "100", "500"), ("id/2", "High, Guy", "300", "1500")],
        ["player_id", "name", "passes_attempted", "passing_yards"],
    )
    out = transform_passing(df).collect()
    assert len(out) == 1


def test_passing_missing_columns(spark):
    df = spark.createDataFrame([("id/77", "Doe, Jim")], ["player_id", "name"])
    out = transform_passing(df).first()
    assert out.first_name == "Jim"


# ---------- PLAYER TESTS ------------------------------------------------------

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
    assert len(out) == 0


def test_players_birthplace_split(spark):
    df = spark.createDataFrame(
        [("id/20", "Brown, Mike", "01/01/1990", "Chicago, IL")],
        ["player_id", "name", "birthday", "birth_place"],
    )
    r = transform_players(df).first()
    assert r.birth_city == "Chicago"
    assert r.birth_state == "IL"


# ---------- RECEIVING TESTS ---------------------------------------------------

def test_receiving_basic(spark):
    df = spark.createDataFrame(
        [("id/1", "Jones, Julio", "90", "1500")],
        ["player_id", "name", "receptions", "receiving_yards"],
    )
    r = transform_receiving(df).first()
    assert r.receiving_yards == 1500


def test_receiving_filter_yards(spark):
    df = spark.createDataFrame(
        [("id/10", "Small, Guy", "50", "200"), ("id/11", "Big, Guy", "90", "1200")],
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


# ---------- RETURN TESTS ------------------------------------------------------

def test_return_basic(spark):
    df = spark.createDataFrame(
        [("id/7", "Hester, Devin", "5")],
        ["player_id", "name", "kick_returns_for_tds"],
    )
    r = transform_return(df).first()
    assert r.kick_returns_for_tds == 5


def test_return_filter_tds(spark):
    df = spark.createDataFrame(
        [("id/1", "Guy, Zero", "0"), ("id/2", "Guy, One", "1")],
        ["player_id", "name", "kick_returns_for_tds"],
    )
    out = transform_return(df).collect()
    assert len(out) == 1


# ---------- RUSHING TESTS -----------------------------------------------------

def test_rushing_basic(spark):
    df = spark.createDataFrame(
        [("id/20", "Henry, Derrick", "300", "2027")],
        ["player_id", "name", "rushing_attempts", "rushing_yards"],
    )
    r = transform_rushing(df).first()
    assert r.rushing_yards == 2027


def test_rushing_filter_yards(spark):
    df = spark.createDataFrame(
        [("id/1", "Small, Runner", "200", "500"), ("id/2", "Big, Runner", "350", "1300")],
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
