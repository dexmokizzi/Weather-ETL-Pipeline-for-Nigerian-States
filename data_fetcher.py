from meteostat import Point, Daily
from datetime import datetime

def fetch_weather_data(state, start_date, end_date):
    """Fetch historical weather data for a given state."""
    location = Point(state['lat'], state['lon'])
    return Daily(location, start_date, end_date).fetch()
