import logging

import pandas as pd
import psycopg2
from psycopg2 import sql

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')


class OlympicDataLoader:
    def __init__(self, db_name, user, password, host='localhost', port='5432'):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

    def connect(self):
        try:
            # Connect to the default 'postgres' database to create the new
            # database
            temp_conn = psycopg2.connect(
                dbname='postgres',  # Connecting to the default database
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            temp_conn.autocommit = True
            with temp_conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"), [
                        self.db_name])
                exists = cursor.fetchone()
                if not exists:
                    cursor.execute(
                        sql.SQL("CREATE DATABASE {}").format(
                            sql.Identifier(
                                self.db_name)))
                    logging.info(
                        f"Database '{
                            self.db_name}' created successfully")
                else:
                    logging.info(f"Database '{self.db_name}' already exists")
            temp_conn.close()

            # Connect to the newly created database
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            logging.info("Database connection established")
        except Exception as e:
            logging.error(f"Error connecting to the database: {e}")
            raise

    def create_schema(self):
        ddl_scripts = """
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
       """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(ddl_scripts)
                self.conn.commit()
                logging.info(
                    "Schema created successfully. Tables are: nocs, sports, games, athletes, teams, events and results.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error creating schema: {e}")
            raise

    def load_data(self, summer_data_path, winter_data_path, regions_data_path):
        cursor = self.conn.cursor()

        # Load the regions data into the NOCs table
        regions_data = pd.read_csv(regions_data_path)
        for _, row in regions_data.iterrows():
            notes_value = None if pd.isna(
                row['notes']) or row['notes'] == 'NaN' else row['notes']
            try:
                cursor.execute("""
                    INSERT INTO nocs (noc_code, region, notes)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (noc_code) DO NOTHING
                """, (row['NOC'], row['region'], notes_value))
                self.log_metadata(cursor, 'nocs', 'INSERT', 'SUCCESS')
            except Exception as e:
                self.conn.rollback()
                self.log_metadata(cursor, 'nocs', 'INSERT', 'FAILURE', str(e))
        self.conn.commit()
        logging.info("Finished inserting data into nocs.")

        # Load the summer and winter data
        olympics_data = pd.concat(
            [pd.read_csv(summer_data_path), pd.read_csv(winter_data_path)])

        # Insert Sports
        for _, row in olympics_data.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO sports (sport_name)
                    VALUES (%s)
                    ON CONFLICT (sport_name) DO NOTHING
                    RETURNING sport_id
                """, (row['Sport'],))
                result = cursor.fetchone()
                if result is not None:
                    sport_id = result[0]
                    self.log_metadata(cursor, 'sports', 'INSERT', 'SUCCESS')
                else:
                    self.log_metadata(cursor, 'sports', 'INSERT', 'DUPLICATE')
            except Exception as e:
                self.log_metadata(
                    cursor, 'sports', 'INSERT', 'FAILURE', str(e))
        self.conn.commit()
        logging.info("Finished inserting data into sports.")

        # Insert Games
        for _, row in olympics_data.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO games (year, season, city)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (year, season, city) DO NOTHING
                    RETURNING game_id
                """, (row['Year'], row['Season'], row['City']))
                result = cursor.fetchone()
                if result is not None:
                    game_id = result[0]
                    self.log_metadata(cursor, 'games', 'INSERT', 'SUCCESS')
                else:
                    self.log_metadata(cursor, 'games', 'INSERT', 'DUPLICATE')
            except Exception as e:
                self.log_metadata(cursor, 'games', 'INSERT', 'FAILURE', str(e))
        self.conn.commit()
        logging.info("Finished inserting data into games.")

        # Insert Athletes
        for _, row in olympics_data.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO athletes (name, sex)
                    VALUES (%s, %s)
                    ON CONFLICT (name, sex) DO NOTHING
                    RETURNING athlete_id
                """, (row['Name'], row['Sex']))
                result = cursor.fetchone()
                if result is not None:
                    athlete_id = result[0]
                    self.log_metadata(cursor, 'athletes', 'INSERT', 'SUCCESS')
                else:
                    self.log_metadata(
                        cursor, 'athletes', 'INSERT', 'DUPLICATE')
            except Exception as e:
                self.log_metadata(
                    cursor, 'athletes', 'INSERT', 'FAILURE', str(e))
        self.conn.commit()
        logging.info("Finished inserting data into athletes.")

        # Insert Teams
        for _, row in olympics_data.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO teams (team_name, noc_id)
                    VALUES (%s, (SELECT noc_id FROM nocs WHERE noc_code = %s))
                    ON CONFLICT (team_name, noc_id) DO NOTHING
                    RETURNING team_id
                """, (row['Team'], row['NOC']))
                result = cursor.fetchone()
                if result is not None:
                    team_id = result[0]
                    self.log_metadata(cursor, 'teams', 'INSERT', 'SUCCESS')
                else:
                    self.log_metadata(cursor, 'teams', 'INSERT', 'DUPLICATE')
            except Exception as e:
                self.log_metadata(cursor, 'teams', 'INSERT', 'FAILURE', str(e))
        self.conn.commit()
        logging.info("Finished inserting data into teams.")

        # Insert Events
        for _, row in olympics_data.iterrows():
            try:
                # Retrieve the correct sport_id
                cursor.execute(
                    "SELECT sport_id FROM sports WHERE sport_name = %s", (row['Sport'],))
                sport_id = cursor.fetchone()[0]

                # Insert Event with the correct sport_id
                cursor.execute("""
                    INSERT INTO events (sport_id, event_name)
                    VALUES (%s, %s)
                    ON CONFLICT (sport_id, event_name) DO NOTHING
                    RETURNING event_id
                """, (sport_id, row['Event']))
                result = cursor.fetchone()
                if result is not None:
                    event_id = result[0]
                    self.log_metadata(cursor, 'events', 'INSERT', 'SUCCESS')
                else:
                    self.log_metadata(cursor, 'events', 'INSERT', 'DUPLICATE')
            except Exception as e:
                self.log_metadata(
                    cursor, 'events', 'INSERT', 'FAILURE', str(e))
        self.conn.commit()
        logging.info("Finished inserting data into events.")

        # Insert Results/Participation
        for _, row in olympics_data.iterrows():
            try:
                # Retrieve the necessary foreign key IDs
                cursor.execute(
                    "SELECT athlete_id FROM athletes WHERE name = %s AND sex = %s",
                    (row['Name'],
                     row['Sex']))
                athlete_id = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT team_id FROM teams WHERE team_name = %s AND noc_id = (SELECT noc_id FROM nocs WHERE noc_code = %s)",
                    (row['Team'],
                     row['NOC']))
                team_id = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT game_id FROM games WHERE year = %s AND season = %s AND city = %s",
                    (row['Year'],
                     row['Season'],
                        row['City']))
                game_id = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT event_id FROM events WHERE event_name = %s AND sport_id = (SELECT sport_id FROM sports WHERE sport_name = %s)",
                    (row['Event'],
                     row['Sport']))
                event_id = cursor.fetchone()[0]

                # Prepare age and medal values
                age_value = int(row['Age']) if pd.notna(row['Age']) else None
                medal_value = None if pd.isna(
                    row['Medal']) or row['Medal'] == '' else row['Medal']

                # Insert into Results with correct IDs
                cursor.execute("""
                    INSERT INTO results (athlete_id, team_id, game_id, event_id, age, medal)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (athlete_id, team_id, game_id, event_id, age_value, medal_value))
                self.log_metadata(cursor, 'results', 'INSERT', 'SUCCESS')
            except Exception as e:
                self.log_metadata(
                    cursor, 'results', 'INSERT', 'FAILURE', str(e))
        self.conn.commit()
        logging.info("Finished inserting data into results.")

        cursor.close()

    def log_metadata(
            self,
            cursor,
            table_name,
            operation,
            status,
            error_message=None):
        cursor.execute(
            sql.SQL("INSERT INTO metadata (table_name, operation, status, error_message) VALUES (%s, %s, %s, %s)"), [
                table_name, operation, status, error_message])

    def close_connection(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")


# Example usage:
loader = OlympicDataLoader(
    db_name='olympic_data',
    user='postgres',
    password='password')
loader.connect()
loader.create_schema()


# Load data into the tables
loader.load_data(
    'data/Athletes_summer_games.csv',
    'data/Athletes_winter_games.csv',
    'data/regions.csv')


loader.close_connection()
logging.info("Exiting program gracefully!!")
