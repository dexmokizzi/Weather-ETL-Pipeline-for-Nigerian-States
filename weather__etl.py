import logging
from datetime import datetime
import pandas as pd
import numpy as np
import warnings
from config import STATES
from db_utils import connect_to_db
from data_fetcher import fetch_weather_data

# Suppress warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Configure logging
logging.basicConfig(
    filename='weather_etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def replace_nan_with_none(row):
    """
    Replace NaN values in a Pandas Series with None.
    """
    return [None if pd.isna(value) else value for value in row]

def load_data_to_db(cursor, state, data):
    """
    Inserts weather data into the database.
    Ensures all NaN values are replaced with None.
    """
    inserted_count = 0
    duplicate_count = 0

    for index, row in data.iterrows():
        date_val = index.date()
        state_name = state['name']

        # Replace NaN with None in the row
        clean_row = replace_nan_with_none(row)

        # Check for duplicates
        cursor.execute('''
            SELECT 1 FROM historical_weather_data
            WHERE state = %s AND date = %s LIMIT 1;
        ''', (state_name, date_val))
        if cursor.fetchone():
            duplicate_count += 1
            continue

        # Insert new record
        try:
            cursor.execute('''
                INSERT INTO historical_weather_data (
                    state, capital, region, date, avg_temperature, min_temperature,
                    max_temperature, precipitation, wind_speed, wind_gust, pressure, sunshine_duration
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                state_name,
                state['capital'],
                state['region'],
                date_val,
                clean_row[0],  # avg_temperature
                clean_row[1],  # min_temperature
                clean_row[2],  # max_temperature
                clean_row[3],  # precipitation
                clean_row[4],  # wind_speed
                clean_row[5],  # wind_gust
                clean_row[6],  # pressure
                clean_row[7]   # sunshine_duration
            ))
            inserted_count += 1
        except Exception as e:
            logging.error(f"Failed to insert data for {state_name} on {date_val}: {e}")

    return inserted_count, duplicate_count

def run_etl():
    """
    Main ETL process.
    """
    logging.info("Starting ETL process...")
    conn = connect_to_db()
    cursor = conn.cursor()

    # Set the date range for historical data
    start_date = datetime(2010, 1, 1)
    end_date = datetime.today()

    for state in STATES:
        logging.info(f"Processing data for {state['name']}...")
        try:
            # Fetch weather data
            data = fetch_weather_data(state, start_date, end_date)

            # Load data into the database
            inserted, duplicates = load_data_to_db(cursor, state, data)
            conn.commit()
            logging.info(f"Data for {state['name']} - Inserted: {inserted}, Duplicates skipped: {duplicates}")
        except Exception as e:
            logging.error(f"Error processing data for {state['name']}: {e}")

    cursor.close()
    conn.close()
    logging.info("ETL process completed.")

if __name__ == "__main__":
    run_etl()
