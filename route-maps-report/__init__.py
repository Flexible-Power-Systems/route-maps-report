import datetime
import logging
import folium
import psycopg2
import geopandas as gpd
import os
import logging
from dotenv import load_dotenv
import pandas as pd
from shapely.geometry import Point, LineString
from pathlib import Path
from folium.plugins import PolyLineTextPath
import warnings
from folium.plugins import Fullscreen
from datetime import datetime, timedelta
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
import azure.functions as func

# Load environment variables from .env file
load_dotenv()

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters from environment variables
host = os.getenv('DB_HOST', "")
dbname = os.getenv('DB_NAME', "")
user = os.getenv('DB_USER', "")
password = os.getenv('DB_PASSWORD', "")
port = os.getenv('DB_PORT', "5432")
script_dir = os.path.dirname(os.path.abspath(__file__))

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
