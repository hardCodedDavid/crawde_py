from flask import Flask, jsonify, request
from tradingview_ta import TA_Handler, Interval
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enables CORS for all routes

@app.route('/api/sentiment', methods=['GET'])
def get_sentiment():
    # Get parameters from query string
    symbol = request.args.get('symbol', 'BTCUSDT')
    exchange = request.args.get('exchange', 'Binance')
    screener = request.args.get('screener', 'crypto')
    interval = request.args.get('interval', Interval.INTERVAL_1_HOUR)
    proxy = request.args.get('proxy')

    try:
        # Set up TradingView TA_Handler with proxy if provided
        analysis = TA_Handler(
            symbol=symbol,
            screener=screener,
            exchange=exchange,
            interval=interval,
            proxies={'http': proxy, 'https': proxy} if proxy else None
        )
        analysis_data = analysis.get_analysis()
        indicators = analysis_data.indicators
        moving_averages = analysis_data.moving_averages

        response = {
            "symbol": symbol,
            "exchange": exchange,
            "screener": screener,
            "interval": interval,
            "indicators": indicators,
            "moving_averages": moving_averages,
        }
        return jsonify(response)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)
