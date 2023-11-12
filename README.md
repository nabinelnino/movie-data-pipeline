# movie-data-pipeline

This Python application processes raw movie data and loads it into a connected database. The main functionality is encapsulated in three modules: `__main__.py`, `process.py`, and `syncdb.py`.

`__main__.py` is entry point of the application, `process.py` contains logics to process and combine the raw input files and database operation is handled by `syncdb.py` files

# Modules

## 1. **main**.py

This module serves as the entry point for the application. It initializes the ProcessClass and DatabaseHandle classes from the process module and executes the main processing logic.

## 2. process.py

This module contains the ProcessClass and DatabaseHandle classes responsible for processing genre json and year json, combining them along with movie_data into CSV file, and loading the data into a database. The key methods include:

convert_genre_to_csv: Converts raw genre data to CSV format.
convert_year_to_csv: Converts raw year data to CSV format.
delete_folder: Deletes a specified folder and its contents.
combine_file: Combines and processes multiple CSV files into a single merged CSV file.

## 3. syncdb.py

The Synchronizer class in this module is responsible for connecting to a database, creating a session, and inserting data into the connected database. The create_connection and sync_main functions are crucial for establishing the database connection and running the main functions, respectively.

## 4. database_handle.py

The DatabaseHandle class in this module is responsible for interacting with database, it uses the SQLAlchemy ORM to communicate with database, it reads model from models folder and create table on the Postgres database based on the connection string.

## Configuration

Configuration settings, such as file paths and database connection details, are stored in the configs/info_config.yaml file. Modify this file according to your specific setup.

# Installation

## Clone the repository:

    - git clone git@github.com:nabinelnino/movie-data-pipeline.git

## Create virtual env

- cd movie-data-pipeline python -m venv <virtual-environment-name>
- activate virtual environment: source <virtual-environment-name>/bin/activate
- install dependencies: pip install -r requirements.txt
- Add database information in the .env file: - cd to database folder and create `.env` variable and add database related information, which is then consumed by Synchronizer class.

- Run the application: python **main**.py
