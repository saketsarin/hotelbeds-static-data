import requests
import json
import pandas as pd
from sqlalchemy import create_engine, inspect, text, Table, MetaData, insert
from datetime import datetime, date
import time
import logging
import sys
import hashlib
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Set up logging with custom formatter
class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(levelname)s - %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger()
for handler in logger.handlers:
    handler.setFormatter(CustomFormatter())

# Hotelbeds API credentials
API_KEY = os.getenv('API_KEY')
SECRET = os.getenv('SECRET')
BASE_URL = 'https://api.test.hotelbeds.com/hotel-content-api/1.0/'

# Database connection
DB_URL = os.getenv('DB_URL')
engine = create_engine(DB_URL)

def generate_signature():
    timestamp = str(int(time.time()))
    return hashlib.sha256((API_KEY + SECRET + timestamp).encode('utf-8')).hexdigest()

def fetch_data(endpoint, params=None):
    headers = {
        'Api-key': API_KEY,
        'X-Signature': generate_signature(),
        'Accept': 'application/json',
        'Accept-Encoding': '*'
    }
    full_url = requests.Request('GET', BASE_URL + endpoint, params=params).prepare().url
    logger.info(f"Fetching data from: {endpoint}")
    response = requests.get(full_url, headers=headers)
    response.raise_for_status()
    return response.json()

def insert_data_chunk(chunk, table_name):
    df = pd.DataFrame([{k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in item.items()} for item in chunk])
    df = df.dropna(axis=1, how='all')
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    table_name = f'hotelbeds_{table_name}' if not table_name.startswith('hotelbeds_') else table_name
    
    if not inspect(engine).has_table(table_name):
        df.head(0).to_sql(table_name, engine, if_exists='replace', index=False)
    
    existing_columns = set(col['name'] for col in inspect(engine).get_columns(table_name))
    missing_columns = set(df.columns) - existing_columns
    with engine.connect() as connection:
        for col in missing_columns:
            connection.execute(text(f'ALTER TABLE {table_name} ADD COLUMN {col} TEXT'))
    
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    with engine.begin() as connection:
        connection.execute(insert(table), df.to_dict(orient='records'))
    
    logger.info(f"Inserted {len(df)} rows into {table_name}")

def update_last_updated_time(table_name):
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO hotelbeds_last_updated_time (table_name, last_updated) VALUES (:table_name, :last_updated) "
                 "ON CONFLICT (table_name) DO UPDATE SET last_updated = :last_updated"),
            {"table_name": table_name, "last_updated": datetime.utcnow()}
        )
    logger.info(f"Updated last updated time for {table_name}")

def fetch_data_generator(endpoint, language='ENG', batch_size=1000, last_update_time=None):
    page = 1
    key = endpoint.split('/')[-1]
    key = {
        'facilitygroups': 'facilityGroups',
        'imagetypes': 'imageTypes',
        'facilitytypologies': 'facilityTypologies',
        'groupcategories': 'groupCategories',
        'boardgroups': 'boardGroups',
        'ratecomments': 'rateComments'
    }.get(key, key)

    while True:
        params = {
            'fields': 'all',
            'language': language,
            'from': (page - 1) * batch_size + 1,
            'to': page * batch_size,
            'lastUpdateTime': last_update_time
        }
        response_data = fetch_data(endpoint, params)
        if response_data and isinstance(response_data, dict) and key in response_data:
            data = response_data[key]
            if data:
                yield data
                logger.info(f"Fetched page {page} for {endpoint}")
                page += 1
                if len(data) < batch_size or (response_data['to'] >= response_data['total']):
                    break
            else:
                break
        else:
            logger.error(f"Unexpected response structure for {endpoint}")
            raise Exception(f"Unexpected response structure for {endpoint}")
        time.sleep(0.25)  # Respect the 4 QPS limit

def create_last_updated_time_table():
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS hotelbeds_last_updated_time (
                table_name TEXT PRIMARY KEY,
                last_updated TIMESTAMP
            )
        """))
    logger.info("Ensured hotelbeds_last_updated_time table exists")

def load_last_update_times():
    with engine.connect() as connection:
        result = connection.execute(text("SELECT table_name, last_updated FROM hotelbeds_last_updated_time"))
        return {row[0]: row[1].strftime('%Y-%m-%d') for row in result}

def save_last_update_times(last_update_times):
    with engine.begin() as connection:
        for table_name, last_updated in last_update_times.items():
            connection.execute(
                text("INSERT INTO hotelbeds_last_updated_time (table_name, last_updated) VALUES (:table_name, :last_updated) "
                     "ON CONFLICT (table_name) DO UPDATE SET last_updated = :last_updated"),
                {"table_name": table_name, "last_updated": datetime.strptime(last_updated, '%Y-%m-%d')}
            )
    logger.info("Saved last update times")

def update_all_endpoints(language='ENG'):
    endpoints_to_tables = {
        'locations/countries': 'countries',
        'locations/destinations': 'destinations',
        'types/rooms': 'rooms',
        'types/boards': 'boards',
        'types/boardgroups': 'boardgroups',
        'types/accommodations': 'accommodations',
        'types/categories': 'categories',
        'types/chains': 'chains',
        'types/classifications': 'classifications',
        'types/facilities': 'facilities',
        'types/facilitygroups': 'facilitygroups',
        'types/facilitytypologies': 'facilitytypologies',
        'types/groupcategories': 'groupcategories',
        'types/issues': 'issues',
        'types/languages': 'languages',
        'types/promotions': 'promotions',
        'types/segments': 'segments',
        'types/imagetypes': 'imagetypes',
        'types/currencies': 'currencies',
        'types/terminals': 'terminals',
        'types/ratecomments': 'ratecomments',
        'hotels': 'hotels'
    }

    last_update_times = load_last_update_times()

    for endpoint, table_name in endpoints_to_tables.items():
        full_table_name = f'hotelbeds_{table_name}'
        last_update_time = last_update_times.get(full_table_name)
        logger.info(f"Checking for updates in {endpoint} since {last_update_time}...")
        
        data_generator = fetch_data_generator(endpoint, language, last_update_time=last_update_time)
        total_items = 0
        
        for chunk in data_generator:
            insert_data_chunk(chunk, table_name)
            total_items += len(chunk)
        
        if total_items > 0:
            logger.info(f"Inserted {total_items} updated items for {endpoint}.")
            last_update_times[full_table_name] = date.today().strftime('%Y-%m-%d')
            update_last_updated_time(full_table_name)
        else:
            logger.info(f"No updates found for {endpoint}.")

    save_last_update_times(last_update_times)
    logger.info("Finished checking all endpoints for updates.")

def main():
    create_last_updated_time_table()
    update_all_endpoints()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        sys.exit(1)