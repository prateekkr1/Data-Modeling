# Olympic Data Management System

This project provides a comprehensive system for managing Olympic data. The database is designed in Third Normal Form (3NF) to ensure data integrity and efficiency. The project includes the SQL schema, Data Definition Language (DDL) scripts, and a Python script to load data into the database.

## 1. SQL Third Normal Form (3NF) Schema and DDL Scripts

The database schema is designed to be in Third Normal Form (3NF). Below are the DDL scripts used to create the database with referential integrity and indexes.

### 1.1. SQL Third Normal Form (3NF) Schema

```sql
-- Athletes Table
CREATE TABLE IF NOT EXISTS athletes (
    athlete_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sex CHAR(1) NOT NULL,
    UNIQUE(name, sex)
);

-- NOC Table
CREATE TABLE IF NOT EXISTS nocs (
    noc_id SERIAL PRIMARY KEY,
    noc_code CHAR(3) NOT NULL UNIQUE,
    region VARCHAR(255),
    notes TEXT
);

-- Teams Table
CREATE TABLE IF NOT EXISTS teams (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(255) NOT NULL,
    noc_id INT REFERENCES nocs(noc_id),
    UNIQUE(team_name, noc_id)
);

-- Games Table
CREATE TABLE IF NOT EXISTS games (
    game_id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    season VARCHAR(6) NOT NULL,
    city VARCHAR(255) NOT NULL,
    UNIQUE(year, season, city)
);

-- Sports Table
CREATE TABLE IF NOT EXISTS sports (
    sport_id SERIAL PRIMARY KEY,
    sport_name VARCHAR(255) NOT NULL UNIQUE
);

-- Events Table
CREATE TABLE IF NOT EXISTS events (
    event_id SERIAL PRIMARY KEY,
    sport_id INT REFERENCES sports(sport_id),
    event_name VARCHAR(255) NOT NULL,
    UNIQUE(sport_id, event_name)
);

-- Results Table
CREATE TABLE IF NOT EXISTS results (
    result_id SERIAL PRIMARY KEY,
    athlete_id INT REFERENCES athletes(athlete_id),
    team_id INT REFERENCES teams(team_id),
    game_id INT REFERENCES games(game_id),
    event_id INT REFERENCES events(event_id),
    age INT,
    medal VARCHAR(50)
);

-- Metadata Table
CREATE TABLE IF NOT EXISTS metadata (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    operation VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 1.2. DDL Scripts for Database with Referential Integrity and Indexes

```sql
-- Athletes Table
CREATE TABLE IF NOT EXISTS athletes (
    athlete_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sex CHAR(1) NOT NULL,
    UNIQUE(name, sex)
);

-- NOC Table
CREATE TABLE IF NOT EXISTS nocs (
    noc_id SERIAL PRIMARY KEY,
    noc_code CHAR(3) NOT NULL UNIQUE,
    region VARCHAR(255),
    notes TEXT
);

-- Teams Table
CREATE TABLE IF NOT EXISTS teams (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(255) NOT NULL,
    noc_id INT REFERENCES nocs(noc_id) ON DELETE CASCADE,
    UNIQUE(team_name, noc_id)
);

-- Games Table
CREATE TABLE IF NOT EXISTS games (
    game_id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    season VARCHAR(6) NOT NULL,
    city VARCHAR(255) NOT NULL,
    UNIQUE(year, season, city)
);

-- Sports Table
CREATE TABLE IF NOT EXISTS sports (
    sport_id SERIAL PRIMARY KEY,
    sport_name VARCHAR(255) NOT NULL UNIQUE
);

-- Events Table
CREATE TABLE IF NOT EXISTS events (
    event_id SERIAL PRIMARY KEY,
    sport_id INT REFERENCES sports(sport_id) ON DELETE CASCADE,
    event_name VARCHAR(255) NOT NULL,
    UNIQUE(sport_id, event_name)
);

-- Results Table
CREATE TABLE IF NOT EXISTS results (
    result_id SERIAL PRIMARY KEY,
    athlete_id INT REFERENCES athletes(athlete_id) ON DELETE CASCADE,
    team_id INT REFERENCES teams(team_id) ON DELETE CASCADE,
    game_id INT REFERENCES games(game_id) ON DELETE CASCADE,
    event_id INT REFERENCES events(event_id) ON DELETE CASCADE,
    age INT,
    medal VARCHAR(50)
);

-- Metadata Table
CREATE TABLE IF NOT EXISTS metadata (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    operation VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for Improved Query Performance
CREATE INDEX idx_athletes_name_sex ON athletes(name, sex);
CREATE INDEX idx_teams_name_noc ON teams(team_name, noc_id);
CREATE INDEX idx_games_year_season_city ON games(year, season, city);
CREATE INDEX idx_events_sport_event ON events(sport_id, event_name);
```

## 2. Referential Integrity

The referential integrity in this database schema is maintained using foreign key constraints, ensuring that relationships between tables are consistent. The ON DELETE CASCADE option is used to automatically delete related records in child tables when a parent record is deleted.

#### Tables and Their Relationships

1. **nocs**: 
   - No dependencies
   - `{noc_id}` is the PK

2. **sports**:
   - No dependencies
   - `{sport_id}` is the PK

3. **games**:
   - No dependencies
   - `{game_id}` is the PK

4. **athletes**:
   - No dependencies
   - `{athlete_id}` is the PK

5. **teams**:
   - Depends on `nocs`
   - `{team_id}` is the PK
   - `{noc_id}` (FK) -> `nocs.{noc_id}`

6. **events**:
   - Depends on `sports`
   - `{event_id}` is the PK
   - `{sport_id}` (FK) -> `sports.{sport_id}`

7. **results**:
   - Depends on `athletes`, `teams`, `games`, and `events`
   - `{result_id}` is the PK
   - `{athlete_id}` (FK) -> `athletes.{athlete_id}`
   - `{team_id}` (FK) -> `teams.{team_id}`
   - `{game_id}` (FK) -> `games.{game_id}`
   - `{event_id}` (FK) -> `events.{event_id}`

## Summary of PK -> FK Relationships:

- **teams**: `{noc_id}` (FK) -> `nocs.{noc_id}`
- **events**: `{sport_id}` (FK) -> `sports.{sport_id}`
- **results**:
  - `{athlete_id}` (FK) -> `athletes.{athlete_id}`
  - `{team_id}` (FK) -> `teams.{team_id}`
  - `{game_id}` (FK) -> `games.{game_id}`
  - `{event_id}` (FK) -> `events.{event_id}`


## 3. Indexes

Indexes are created to ensure faster query performance on commonly accessed columns. These indexes support efficient lookups and enforce uniqueness where necessary.

## 4. How to Run the Project

### 4.1. Prerequisites

Ensure that you have the following installed:

* Python 3.x
* PostgreSQL
* psycopg2 and pandas Python libraries (included in requirements.txt)

### 4.2. Running the project

* Ensure the Shell Script is Executable:
    * The shell script run_olympic_data_load.sh has already been made executable. If not you can run "chmod +x run_olympic_data_load.sh" to make it executable.

* Run the Shell Script:

    ```bash
    ./run_olympic_data_load.sh
    ```

* Verify the Data:

    Check the database to ensure that all tables have been populated correctly.

* Review Metadata:

    The metadata table will log the success, duplicates, and failures of data insertion at the record level.


## 5. Descriptions of Each Entity in the Database

* **athletes**: Stores athlete information, including name and sex. Each athlete has a unique identifier.
* **nocs**: Stores National Olympic Committees (NOCs) details, including noc_code, region, and any notes.
* **teams**: Represents teams participating in the Olympics, linked to NOCs via noc_id.
* **games**: Stores details of each Olympic event, including year, season, and city.
* **sports**: Contains information about different sports.
* **events**: Represents specific events within a sport, linked to sports via sport_id.
* **results**: Tracks the results of athletes in various events, including age, medal, and links to athletes, teams, games, and events.
* **metadata**: Logs the success, failure, and duplication of data loading processes at the record level, providing detailed auditing capabilities.

## 6. Project Files

This project includes the following files:

1. **olympic_data**: 
   - This is the database dump, which is a backup created using pgAdmin4.
   - To restore this database, create a new database in pgAdmin and use the "Restore" option to load this dump file.

2. **olympic_data_loader.py**:
   - This Python library is responsible for loading the Olympic data sources into the PostgreSQL database.
   - It includes functionality for creating the schema, loading data, and maintaining referential integrity.

3. **run_olympic_data_load.sh**:
   - A shell script that automates the process of setting up the environment, creating the database schema, and loading the data into the tables.
   - Ensure this script is executable before running it:
     ```bash
     chmod +x run_olympic_data_load.sh
     ```
   - To run the script, simply execute:
     ```bash
     ./run_olympic_data_load.sh
     ```

4. **requirements.txt**:
   - This file lists all the Python dependencies required to run the project.
   - Install the dependencies using:
     ```bash
     pip install -r requirements.txt
     ```

5. **README.md**:
   - This file shows how to run the project, includes DDL scripts, the 3NF schema, referential integrity, and a description of each entity in the database.


# Notes

## Platform Compatibility

This program has been tested on macOS. If you are running this on Windows, you might need to make necessary changes, particularly in paths, shell scripts, and environment setup steps. Ensure that your environment variables and dependencies are correctly configured for Windows.

## Database Configuration

Before running the project, ensure that the database name, username, and password in the `olympic_data_loader.py` script are set according to your PostgreSQL configuration. Update the following fields in the script:

```python
loader = OlympicDataLoader(
    db_name='your_database_name',
    user='your_username',
    password='your_password'
)
```