from flask import Flask, jsonify, request
from tradingview_ta import TA_Handler, Interval
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/api/sentiment', methods=['GET'])
def get_sentiment():
    # Get parameters from query string
    symbol = request.args.get('symbol', 'BTCUSDT')  # Default to BTCUSDT
    exchange = request.args.get('exchange', 'Binance')  # Default to Binance
    screener = request.args.get('screener', 'crypto')  # Default to crypto
    interval = request.args.get('interval', Interval.INTERVAL_1_HOUR)  # Default to 1 hour

    try:
        analysis = TA_Handler(
            symbol=symbol,
            screener=screener,
            exchange=exchange,
            interval=interval
        )
        indicators = analysis.get_analysis().indicators
        moving_averages = analysis.get_analysis().moving_averages

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