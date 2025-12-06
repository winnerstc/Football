# Football Data Engineering Pipeline

## Overview
This project implements an end-to-end data engineering pipeline for NFL football statistics. It supports full and incremental data loads, structured transformations, automated testing, CI/CD with Jenkins, and loading curated datasets into PostgreSQL for downstream analytics and reporting.

The pipeline processes multiple statistical domains including passing, rushing, receiving, defensive, kicking, player, and return data, following production-style data engineering best practices.

---

## Architecture Summary
Source CSV Files → PySpark Transformations → Validation & Testing → PostgreSQL

- Source: NFL statistics CSV files  
- Processing Engine: PySpark  
- Load Strategy: Full Load and Incremental Load  
- Destination: PostgreSQL  
- Automation: Jenkins CI/CD  
- Quality Control: Unit Tests  

---

## Repository Structure

Football/
├── CSV_Files/  
├── Incremental/  
│   ├── nfl_defensive_inc.py  
│   ├── nfl_kicking_inc.py  
│   ├── nfl_passing_inc.py  
│   ├── nfl_players_inc.py  
│   ├── nfl_receiving_inc.py  
│   ├── nfl_returns_inc.py  
│   └── nfl_rushing_inc.py  
├── Transforms/  
│   ├── transform_defensive.py  
│   ├── transform_kicking.py  
│   ├── transform_passing.py  
│   ├── transform_players.py  
│   ├── transform_receiving.py  
│   ├── transform_return.py  
│   └── transform_rushing.py  
├── load_into_postgres/  
│   ├── loadintopostgres.py  
│   ├── Basic_Stats.csv  
│   ├── Career_Stats_Defensive.csv  
│   ├── Career_Stats_Field_Goal_Kickers.csv  
│   ├── Career_Stats_Passing.csv  
│   ├── Career_Stats_Receiving.csv  
│   └── Career_Stats_Rushing.csv  
├── unit tests/  
│   ├── test_all_7_transforms.py  
│   └── full_test_nfl_21_tests.py  
├── JenkinsFile  
├── common_utils.py  
├── requirements.txt  
├── sqoop commands.txt  
└── README.md  

---

## Key Features
- Domain-based NFL data transformations  
- Incremental and full load processing  
- Defensive filtering and data validation  
- PostgreSQL loading layer  
- Automated CI/CD using Jenkins  
- Comprehensive unit testing  
- Modular and reusable code design  

---

## Technologies Used
- Python  
- PySpark  
- PostgreSQL  
- Jenkins  
- Git & GitHub  

---

## Setup Instructions

### Clone the Repository
git clone https://github.com/winnerstc/Football.git  
cd Football  
git checkout Development-Branch  

### Install Dependencies
pip install -r requirements.txt  

---

## Running the Pipeline

### Run Full Load Transformations
spark-submit Transforms/transform_passing.py  
spark-submit Transforms/transform_rushing.py  

### Run Incremental Loads
spark-submit Incremental/nfl_passing_inc.py <last_processed_timestamp>  

---

## Load Data into PostgreSQL
Update database credentials inside load_into_postgres/loadintopostgres.py, then run:
python load_into_postgres/loadintopostgres.py  

---

## Testing
Run all unit tests:
pytest unit\ tests/  

Tests validate:
- Schema correctness  
- Data filtering logic  
- Incremental boundaries  
- Transformation completeness  

---

## CI/CD Pipeline
The JenkinsFile automates:
- Dependency installation  
- Execution of full and incremental jobs  
- Test execution  
- Error handling and job failure reporting  

This enables hands-off, repeatable pipeline execution in a production-style environment.

---

## Author
Sanket Patel / sanketpateltechconsulting-droid  

Primary contributions include incremental load logic, defensive data filtering, transformation correctness, and automated testing enhancements.

---

## Use Cases
- Sports analytics platforms  
- Data engineering portfolio project  
- Interview-ready ETL pipeline demonstration  
- Data warehousing practice  

---

## Future Enhancements
- Workflow orchestration with Apache Airflow  
- Cloud storage integration (S3 / GCS)  
- Partitioned PostgreSQL tables  
- Data quality monitoring dashboards  

---

This project demonstrates real-world data engineering workflows using production-style patterns.
