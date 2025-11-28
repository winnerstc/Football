**Author:** Marc Hanna 
# Project Title
Kick Return Career Stats ETL Process
# Objective
This ETL process extracts NFL kick return career statistics from a CSV file and processes it through multiple stages:

1) Load the raw CSV into a database (DBeaver / PostgreSQL).

2) Import the table into HDFS using Sqoop.

3) Organize data into Raw → Silver → Gold layers in HDFS. **Silver:** Clean and standardize the data. **Gold:** Transform data into a Star Schema with fact and dimension tables. Compute high-value metrics for analysis.

# Database/App Flow
## Database:
1) Opened DBeaver and connected to the mhanna_db database on Host 18.134.163.221.

2) Created a _career_stats_kick_return_ table.

3) Used the “Import CSV File” command to load [the kick return stats football dataset](https://www.kaggle.com/datasets/kendallgillies/nflstatistics?select=Career_Stats_Kick_Return.csv) CSV into the _career_stats_kick_return_ table.

Example Columns:
player_id, last_name, first_name, middle_initial, year, team, games_played, fair_catches, returns, yards_returned, yards_per_return, returns_longer_20, returns_longer_40, returns_for_tds, fumbles, longest_return, position

## Importing with Sqoop
4) **Sqoop Import to HDFS**:

* **full load**:
```
 sqoop import --connect jdbc:postgresql://18.134.163.221:5432/mhanna_db --username Consultants --password WelcomeItc@2022 --table career_stats_kick_return --m 1 --target-dir /tmp/DE011025/marc/sqoop/career_stats_kick_return --fields-terminated-by ',' --lines-terminated-by '\n' --null-string '\\N' --null-non-string '\\N' --as-textfile
 ```

* **incremental load** (just for practice):
```
sqoop import --connect jdbc:postgresql://18.134.163.221:5432/mhanna_db --username Consultants --password WelcomeItc@2022 --table career_stats_kick_return --m 1 --target-dir /tmp/DE011025/marc/sqoop/career_stats_kick_return --fields-terminated-by ',' --lines-terminated-by '\n' --null-string '\\N' --null-non-string '\\N' --as-textfile --incremental append --check-column Player Id last-value 1000 
```

**Arguments**:

--m 1: Use a single mapper for initial import.

--target-dir: HDFS location for raw data.

**Original load** (from load command): /tmp/DE011025/marc/sqoop/career_stats_kick_return

## Python Spark Script for ETL
5) 
    1. Grab CSV data from temporary raw location and Convert raw data to a Pyspark Dataframe.
    2. **raw-> Bronze:** Copy from temporary raw location to **Bronze location (CSV):** _hdfs:///tmp/de011025/marc/raw/staging_career_stats_kick_return_
6) Data Cleaning / **Bronze -> Silver Transformation**:

    1. **Clean Data:** Remove duplicates, Standardize string columns (trim, uppercase). Replace missing numeric values with 0.
    2. **Enrich data** for later analysis (e.g., compute derived columns: yards_per_return and long_return_flag).
    3. Write to **Silver location (Parquet):** _/tmp/de011025/name/Silver/silver_staging_clean_

7) **Silver -> Gold Transformation:** 
   1. Perform aggregation transformations on the cleaned kick_return_stats (fact table dataframe), and player, team and year
   (dimension table dataframes) 
   2. write to **Gold location (Parquet):** _/tmp/DE011025/marc/Gold/gold_fact_kick_return_stats_

## Star Schema Design
### Dimension Tables:

**dim_player:**

Surrogate key: player_key

Attributes: player_id, first_name, last_name, position, career_span, rookie_flag, total_yards, total_returns

**dim_team:**

* Surrogate key: 

    team_key
* Attributes: 

    team_name

**dim_year**

* Surrogate key: 

    year_key
* Attributes: 

    year

### Fact Table:

**fact_kick_return_stats**

* Foreign Keys:

    player_key, team_key, year_key
* Attributes (from original data): 

    player_key, team_key, year_key, games_played, returns, yards_returned, yards_per_return, returns_longer_20, returns_longer_40, returns_for_tds, fumbles, longest_return

* Derived attributes: 

    long_return_flag (1 if >40 yards), success_rate_20plus, success_rate_40plus, turnover_risk, weighted_return_score, cumulative_yards

### Schema Diagram:

        dim_player      dim_team       dim_year
           |                |              |
           ---------------------------------
                        fact_kick_return_stats
