Description

Football is a data pipeline and analysis project designed to determine which colleges produce the most successful NFL players. It integrates data ingestion, transformation, and loading into a PostgreSQL database, supporting statistical and visual insights into college-to-NFL transitions.

Features

Ingest and transform raw CSV data from various sources

Load cleaned data into PostgreSQL

Utility functions for common ETL operations

Jenkins CI support for automated builds

Unit-tested modules for reliability

Installation

Clone the repository:

git clone https://github.com/winnerstc/Football.git
cd Football


Set up a virtual environment:

python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows


Install dependencies:

pip install -r requirements.txt

Usage

Run the ETL pipeline:

python load_into_postgres/load_data.py


Transform data:

python Transforms/transform_stats.py


Run unit tests:

python -m unittest discover unit\ tests/

Directory Structure
Football/
├── CSV_Files/             # Source data files
├── Transforms/            # Data transformation scripts
├── load_into_postgres/    # ETL scripts to load data
├── unit tests/            # Test modules
├── JenkinsFile            # Jenkins CI config
├── requirements.txt       # Python dependencies

Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change or improve.

License

This project is licensed under the MIT License. See the LICENSE
 file for details.

Changelog

See CHANGELOG.md
 (to be created) for version history.
﻿
Canada
s…
 

