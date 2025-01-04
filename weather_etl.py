import psycopg2
from psycopg2 import sql
from datetime import datetime
import logging
from meteostat import Point, Daily
import warnings
from state_metadata import STATES

# -----------------------------
# Suppress FutureWarnings
# -----------------------------
warnings.simplefilter(action='ignore', category=FutureWarning)

# -----------------------------
# Logging Configuration
# -----------------------------
logging.basicConfig(
    filename='weather_etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# -----------------------------
# Configuration
# -----------------------------

# Log a brief list of states at startup (only once).
logging.info("Starting Weather ETL Script.")
state_names = [state['name'] for state in STATES]
logging.info(f"Configured States: {state_names}")

# Database connection details
DB_NAME = 'weather_data'
DB_USER = 'postgres'
DB_PASSWORD = 'savage'  # Replace with your actual password
DB_HOST = 'localhost'
DB_PORT = '5432'

# -----------------------------
# Database Initialization
# -----------------------------
def initialize_database():
    """
    Initializes the database and creates the required table if it doesn't exist.
    Ensures the schema is ready before loading data.
    """
    try:
        logging.info("Initializing database...")

        # Connect to PostgreSQL default database
        conn = psycopg2.connect(
            dbname='postgres',
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Create database if it doesn't exist
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f"CREATE DATABASE {DB_NAME};")
            logging.info(f"Database '{DB_NAME}' created successfully.")

        cursor.close()
        conn.close()

        # Connect to the created database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Create table for weather data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS historical_weather_data (
                id SERIAL PRIMARY KEY,
                state VARCHAR(50),
                capital VARCHAR(50),
                region VARCHAR(50),
                date DATE,
                avg_temperature REAL,
                min_temperature REAL,
                max_temperature REAL,
                precipitation REAL,
                wind_speed REAL,
                wind_gust REAL,
                pressure REAL,
                sunshine_duration REAL,
                created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (state, date) -- Prevents duplicate entries for the same state and date
            )
        ''')
        conn.commit()
        cursor.close()
        conn.close()

        logging.info("Database and table initialized successfully.")
    except Exception as e:
        logging.error(f"Database initialization error: {e}")
        raise

# -----------------------------
# Fetch and Load Data
# -----------------------------
def fetch_and_load():
    """
    Fetches weather data from Meteostat, processes it, and loads it into the database.
    - Checks if (state, date) already exists.
      If it does, we skip inserting that row.
      If not, we insert the new row.
    - Logs reduced by aggregating duplicates vs. inserted counts.
    """
    try:
        logging.info("Starting data fetch and load process...")

        # Connect to the database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Set the date range for historical data
        start_date = datetime(2010, 1, 1)
        end_date = datetime.today()

        # Process each state in the list
        for state in STATES:
            logging.info(f"Fetching data for {state['name']}...")
            location = Point(state['lat'], state['lon'])

            # Fetch daily weather data
            data = Daily(location, start_date, end_date).fetch()

            # Counters to reduce log noise
            inserted_count = 0
            duplicate_count = 0

            # Iterate through each date in the DataFrame
            for index, row in data.iterrows():
                date_val = index.date()
                state_name = state['name']

                # Check if this (state, date) already exists
                cursor.execute('''
                    SELECT 1 
                    FROM historical_weather_data
                    WHERE state = %s 
                      AND date = %s
                    LIMIT 1;
                ''', (state_name, date_val))
                record_exists = cursor.fetchone()

                # If record exists, skip
                if record_exists:
                    duplicate_count += 1
                    continue

                # Otherwise, insert a new record
                cursor.execute('''
                    INSERT INTO historical_weather_data (
                        state,
                        capital,
                        region,
                        date,
                        avg_temperature,
                        min_temperature,
                        max_temperature,
                        precipitation,
                        wind_speed,
                        wind_gust,
                        pressure,
                        sunshine_duration
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                (
                    state_name,
                    state['capital'],
                    state['region'],
                    date_val,
                    row['tavg'],
                    row['tmin'],
                    row['tmax'],
                    row['prcp'],
                    row['wspd'],
                    row['wpgt'],
                    row['pres'],
                    row['tsun']
                ))
                inserted_count += 1

            # Commit after each state's data is processed
            conn.commit()
            logging.info(f"Data for {state['name']} done. Inserted: {inserted_count}, Duplicates skipped: {duplicate_count}")

        # Close connections
        cursor.close()
        conn.close()

        logging.info("Data loading completed successfully!")
    except Exception as e:
        logging.error(f"Error fetching or loading data: {e}")
        logging.info(f"Error: {e}")

# -----------------------------
# Main Execution
# -----------------------------
if __name__ == '__main__':
    """
    Main execution point for the ETL pipeline. Initializes the database 
    and fetches data for each state listed in the configuration.
    """
    # Initialize the database and tables
    initialize_database()

    # Fetch and load data into PostgreSQL
    fetch_and_load()

    # Final completion message
    print("ETL process completed successfully.")
