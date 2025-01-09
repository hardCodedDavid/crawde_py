import requests
import mysql.connector
from mysql.connector import pooling
import time
import logging
from logging.handlers import RotatingFileHandler

# Set up logging with rotation to prevent large log files
log_handler = RotatingFileHandler('script.log', maxBytes=.2 * 1024 * 1024, backupCount=1)
logging.basicConfig(level=logging.INFO, handlers=[log_handler])

# MySQL database connection pooling setup
DB_CONFIG = {
    'host': 'localhost',
    'database': 'itrust_test',
    'user': 'itrust_test_db',
    'password': 'Itrust@2025'
}

# Create a connection pool with a size of 5 connections
db_pool = pooling.MySQLConnectionPool(pool_name="my_pool", pool_size=5, **DB_CONFIG)

# API endpoint
API_URL = "https://orionterminal.com/api/screener"


def get_data_from_api():
    """Fetch data from the API with error handling."""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None


def check_symbol_exists(symbol, cursor):
    """Check if the symbol already exists in the database."""
    try:
        query = "SELECT COUNT(1) FROM coins WHERE slug = %s"
        cursor.execute(query, (symbol,))
        result = cursor.fetchone()
        return result[0] > 0
    except Exception as e:
        logging.error(f"Error checking if {symbol} exists in database: {e}")
        return False


def update_coin_in_database(symbol, metrics, cursor):
    """Update coin data in the database."""
    try:
        # Extract relevant fields from metrics
        price = metrics['11']
        change_5m = metrics['3']
        change_15m = metrics['0']
        change_1h = metrics['2']
        change_8h = metrics['4']
        change_1d = metrics['1']
        volatility_5m = metrics['14']
        volatility_15m = metrics['12']
        volatility_1h = metrics['13']
        ticks_5m = metrics['17']
        ticks_15m = metrics['15']
        ticks_1h = metrics['16']
        vdelta_5m = metrics['21']
        vdelta_15m = metrics['18']
        vdelta_1h = metrics['20']
        vdelta_8h = metrics['22']
        vdelta_1d = metrics['19']
        volume_5m = metrics['26']
        volume_15m = metrics['23']
        volume_1h = metrics['25']
        volume_8h = metrics['27']
        volume_1d = metrics['24']
        oi_change_5m = metrics['50'][0] if isinstance(metrics['50'], list) else None
        oi_change_15m = metrics['6'][0] if isinstance(metrics['6'], list) else None
        oi_change_1h = metrics['8'][0] if isinstance(metrics['8'], list) else None
        oi_change_1d = metrics['9'][0] if isinstance(metrics['9'], list) else None
        oi_change_8h = metrics['7'][0] if isinstance(metrics['7'], list) else None
        funding_rate = metrics['5']
        open_interest = metrics['10'][0] if isinstance(metrics['10'], list) else None
        marketcap = metrics['40']
        btc_correlation_1d = metrics['42']
        eth_correlation_1d = metrics['43']

        # Check if the symbol exists before updating
        if check_symbol_exists(symbol, cursor):
            # Prepare SQL query to update the data
            query = """
            UPDATE coins 
            SET price = %s, change_5m = %s, change_15m = %s, change_1h = %s, change_8h = %s, change_1d = %s,
                volatility_5m = %s, volatility_15m = %s, volatility_1h = %s, ticks_5m = %s, ticks_15m = %s, ticks_1h = %s,
                vdelta_5m = %s, vdelta_15m = %s, vdelta_1h = %s, vdelta_8h = %s, vdelta_1d = %s,
                volume_5m = %s, volume_15m = %s, volume_1h = %s, volume_8h = %s, volume_1d = %s,
                oi_change_5m = %s, oi_change_15m = %s, oi_change_1h = %s, oi_change_1d = %s, oi_change_8h = %s,
                funding_rate = %s, open_interest = %s, marketcap = %s, btc_correlation_1d = %s, eth_correlation_1d = %s
            WHERE slug = %s
            """
            cursor.execute(query, (
                price, change_5m, change_15m, change_1h, change_8h, change_1d,
                volatility_5m, volatility_15m, volatility_1h, ticks_5m, ticks_15m, ticks_1h,
                vdelta_5m, vdelta_15m, vdelta_1h, vdelta_8h, vdelta_1d,
                volume_5m, volume_15m, volume_1h, volume_8h, volume_1d,
                oi_change_5m, oi_change_15m, oi_change_1h, oi_change_1d, oi_change_8h,
                funding_rate, open_interest, marketcap, btc_correlation_1d, eth_correlation_1d, symbol
            ))
            logging.info(f"Updated {symbol} in the database with complete data.")
        else:
            logging.info(f"Symbol {symbol} does not exist in the database. Skipping update.")
    except Exception as e:
        logging.error(f"Error updating {symbol} in database: {e}")


def update_database():
    """Fetch data from API and update the database."""
    data = get_data_from_api()
    if not data:
        return

    try:
        # Get a connection from the connection pool
        connection = db_pool.get_connection()
        cursor = connection.cursor()

        # Process each symbol and update the database
        for symbol, metrics in data.items():
            update_coin_in_database(symbol, metrics, cursor)

        # Commit changes to the database and close the connection
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Database updated successfully!")
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")


# Run the update process periodically
if __name__ == "__main__":
    print("Starting periodic updates...")
    while True:
        start_time = time.time()
        update_database()
        elapsed_time = time.time() - start_time
        print(f"Elapsed time for update: {elapsed_time:.2f} seconds")
        time.sleep(max(0, 2 - elapsed_time))  # Update interval of 2 Seconds