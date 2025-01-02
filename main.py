import requests
import mysql.connector
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# MySQL database connection details
DB_CONFIG = {
    'host': 'localhost',
    'database': 'itrust_test',
    'user': 'itrust_test_db',
    'password': 'Itrust@2025'
}

# API endpoint
API_URL = "https://orionterminal.com/api/screener"


def get_data_from_api():
    try:
        # Call the API to fetch data
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None


def check_symbol_exists(symbol, cursor):
    try:
        # Query to check if the symbol exists in the database by slug
        query = "SELECT COUNT(1) FROM coins WHERE slug = %s"
        cursor.execute(query, (symbol,))
        result = cursor.fetchone()
        return result[0] > 0  # Return True if the symbol exists, otherwise False
    except Exception as e:
        logging.error(f"Error checking if {symbol} exists in database: {e}")
        return False

def update_coin_in_database(symbol, metrics, cursor):
    try:
        # Extract only the required data for price and change_15m
        price = metrics['11']
        change_5m = metrics['3']
        change_15m = metrics['0']
        marketcap = metrics['40']
        open_interest = metrics['10'][1]
        oi_change_15m = metrics['6'][1]

        # Check if the symbol exists in the database before updating
        if check_symbol_exists(symbol, cursor):
            # Prepare SQL query to update only price and change_15m using the correct column
            query = """
            UPDATE coins 
            SET price = %s, change_5m = %s, change_15m = %s, marketcap = %s, open_interest = %s, oi_change_15m = %s
            WHERE slug = %s
            """
            # Execute query
            cursor.execute(query, (price, change_5m, change_15m, marketcap, open_interest, oi_change_15m, symbol))
            logging.info(f"Updated {symbol} in the database with price and change_15m.")
        else:
            logging.info(f"Symbol {symbol} does not exist in the database. Skipping update.")
    except Exception as e:
        logging.error(f"Error updating {symbol} in database: {e}")

def update_database():
    # Fetch data from API
    data = get_data_from_api()
    if not data:
        return

    try:
        # Set up database connection
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Process each symbol and update the database
        for symbol, metrics in data.items():
            update_coin_in_database(symbol, metrics, cursor)

        # Commit changes to database
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Database updated successfully!")
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")


# Run the update process
if __name__ == "__main__":
    print("Starting periodic updates...")
    while True:
        start_time = time.time()
        update_database()
        elapsed_time = time.time() - start_time
        print(f"Elapsed time for update: {elapsed_time:.2f} seconds")
        time.sleep(max(0, 2 - elapsed_time))  # Ensures updates run every 2 seconds if no delay occurs
