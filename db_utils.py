import psycopg2
from config import DB_CONFIG

def connect_to_db():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise RuntimeError(f"Database connection failed: {e}")
