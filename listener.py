import pymysql
import asyncio
import websockets
import json
from decimal import Decimal
from collections import defaultdict

# Initialize MySQL connection
connection = pymysql.connect(host='localhost', user='itrust_test_db', password='Itrust@2025', db='itrust_test')

# Set the transaction isolation level
with connection.cursor() as cursor:
    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")

# Global variable to store the last prices for each symbol and set to hold connected clients
last_data = defaultdict(dict)
connected_clients = set()

def convert_decimal_to_float(data):
    """Recursively convert Decimal values to float in a dictionary."""
    if isinstance(data, dict):
        return {key: convert_decimal_to_float(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_decimal_to_float(item) for item in data]
    elif isinstance(data, Decimal):
        return float(data)
    else:
        return data

async def send_price_update(symbol, data):
    """Send data update to all connected clients."""
    if connected_clients:  # Check if there are any clients connected
        message = json.dumps({
            "symbol": symbol,
            "data": data
        })
        print(f"Sending WebSocket event: {message}")

        # Broadcast the message to all clients
        await asyncio.gather(*[client.send(message) for client in connected_clients])

def fetch_latest_data():
    """Fetch the latest data for all symbols from the database."""
    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT 
                symbol, price, change_5m, change_15m, change_1h, change_8h, change_1d,
                volatility_5m, volatility_15m, volatility_1h,
                ticks_5m, ticks_15m, ticks_1h,
                vdelta_5m, vdelta_15m, vdelta_1h, vdelta_8h, vdelta_1d,
                volume_5m, volume_15m, volume_1h, volume_8h, volume_1d,
                oi_change_5m, oi_change_15m, oi_change_1h, oi_change_1d, oi_change_8h,
                funding_rate, open_interest, marketcap, btc_correlation_1d, eth_correlation_1d
            FROM coins
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            return {
                symbol: {
                    "price": convert_decimal_to_float(price),
                    "change_5m": convert_decimal_to_float(change_5m),
                    "change_15m": convert_decimal_to_float(change_15m),
                    "change_1h": convert_decimal_to_float(change_1h),
                    "change_8h": convert_decimal_to_float(change_8h),
                    "change_1d": convert_decimal_to_float(change_1d),
                    "volatility_5m": convert_decimal_to_float(volatility_5m),
                    "volatility_15m": convert_decimal_to_float(volatility_15m),
                    "volatility_1h": convert_decimal_to_float(volatility_1h),
                    "ticks_5m": convert_decimal_to_float(ticks_5m),
                    "ticks_15m": convert_decimal_to_float(ticks_15m),
                    "ticks_1h": convert_decimal_to_float(ticks_1h),
                    "vdelta_5m": convert_decimal_to_float(vdelta_5m),
                    "vdelta_15m": convert_decimal_to_float(vdelta_15m),
                    "vdelta_1h": convert_decimal_to_float(vdelta_1h),
                    "vdelta_8h": convert_decimal_to_float(vdelta_8h),
                    "vdelta_1d": convert_decimal_to_float(vdelta_1d),
                    "volume_5m": convert_decimal_to_float(volume_5m),
                    "volume_15m": convert_decimal_to_float(volume_15m),
                    "volume_1h": convert_decimal_to_float(volume_1h),
                    "volume_8h": convert_decimal_to_float(volume_8h),
                    "volume_1d": convert_decimal_to_float(volume_1d),
                    "oi_change_5m": convert_decimal_to_float(oi_change_5m),
                    "oi_change_15m": convert_decimal_to_float(oi_change_15m),
                    "oi_change_1h": convert_decimal_to_float(oi_change_1h),
                    "oi_change_1d": convert_decimal_to_float(oi_change_1d),
                    "oi_change_8h": convert_decimal_to_float(oi_change_8h),
                    "funding_rate": convert_decimal_to_float(funding_rate),
                    "open_interest": convert_decimal_to_float(open_interest),
                    "marketcap": convert_decimal_to_float(marketcap),
                    "btc_correlation_1d": convert_decimal_to_float(btc_correlation_1d),
                    "eth_correlation_1d": convert_decimal_to_float(eth_correlation_1d)
                } for (
                    symbol, price, change_5m, change_15m, change_1h, change_8h, change_1d,
                    volatility_5m, volatility_15m, volatility_1h,
                    ticks_5m, ticks_15m, ticks_1h,
                    vdelta_5m, vdelta_15m, vdelta_1h, vdelta_8h, vdelta_1d,
                    volume_5m, volume_15m, volume_1h, volume_8h, volume_1d,
                    oi_change_5m, oi_change_15m, oi_change_1h, oi_change_1d, oi_change_8h,
                    funding_rate, open_interest, marketcap, btc_correlation_1d, eth_correlation_1d
                ) in results
            }
    except Exception as e:
        print(f"Error fetching data from the database: {e}")
        return {}

async def monitor_price():
    """Monitor price changes and send WebSocket event on change."""
    global last_data
    while True:
        try:
            current_data = fetch_latest_data()
            print(f"Current data fetched: {current_data}")  # Debug: show all fetched data

            # Check each symbol for data changes
            for symbol, current_values in current_data.items():
                last_values = last_data.get(symbol)

                if last_values != current_values:  # Only update if there's a change
                    print(f"Data for {symbol} changed: {current_values} ✅")
                    await send_price_update(symbol, current_values)
                    last_data[symbol] = current_values
                else:
                    print(f"No change in data for {symbol}.")  # Debug: indicate no change

            # Remove old symbols that are no longer in the database
            for symbol in list(last_data.keys()):
                if symbol not in current_data:
                    print(f"Data for {symbol} removed.")
                    del last_data[symbol]

            # Sleep for a more reasonable time before checking again (e.g., 1 seconds)
            await asyncio.sleep(1)
        except Exception as e:
            print(f"Error in monitor_price: {e}")
            await asyncio.sleep(5)

async def echo(websocket, path):
    """Handle incoming WebSocket connections."""
    print(f"New client connected: {websocket.remote_address}")
    connected_clients.add(websocket)  

    try:
        async for message in websocket:
            print(f"Received WebSocket message: {message}")
            # Optional: echo back the received message
            await websocket.send(f"Server received: {message}")
    except websockets.exceptions.ConnectionClosed as e:
        print(f"Client disconnected: {websocket.remote_address}, reason: {e}")
    finally:
        connected_clients.remove(websocket)  # Remove client when they disconnect

async def start_websocket_server():
    """Start WebSocket server."""
    server = await websockets.serve(echo, "localhost", 6789)  # Port 6789 matches the client
    print("WebSocket server running on ws://localhost:6789")
    await server.wait_closed()

async def main():
    """Run both the WebSocket server and the price monitor concurrently."""
    await asyncio.gather(
        start_websocket_server(),
        monitor_price(),
    )

if __name__ == '__main__':
    try:
        asyncio.run(main())  # Run both tasks concurrently
    except KeyboardInterrupt:
        print("Monitoring stopped.")
    finally:
        connection.close()  # Ensure MySQL connection is closed properly