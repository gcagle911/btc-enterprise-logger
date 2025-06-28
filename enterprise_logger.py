import requests
import csv
import time
from datetime import datetime
from flask import Flask, send_file
import threading

CSV_FILE = 'enterprise_data.csv'  # ← New CSV filename

# Create CSV if not exists
try:
    with open(CSV_FILE, 'x', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'price', 'bid', 'ask', 'spread', 'volume'])
except FileExistsError:
    pass

def fetch_coinbase_data():
    ticker_url = 'https://api.coinbase.com/v2/exchange-rates?currency=BTC'
    book_url = 'https://api.exchange.coinbase.com/products/BTC-USD/book?level=1'

    ticker = requests.get(ticker_url).json()
    book = requests.get(book_url).json()

    price = float(ticker['data']['rates']['USD'])

    if 'bids' in book and 'asks' in book and book['bids'] and book['asks']:
        bid = float(book['bids'][0][0])
        ask = float(book['asks'][0][0])
    else:
        bid = price * 0.999
        ask = price * 1.001

    spread = ask - bid
    volume = 0.0
    timestamp = datetime.utcnow().isoformat()

    return [timestamp, price, bid, ask, spread, volume]

def log_data():
    try:
        data = fetch_coinbase_data()
        with open(CSV_FILE, 'a', newline='') as f:
            csv.writer(f).writerow(data)
        print(f"Logged: {data}")
    except Exception as e:
        print(f"Error: {e}")

# --- Flask server setup ---
app = Flask(__name__)

@app.route('/')
def home():
    return '<h1>Enterprise BTC Logger</h1><a href="/enterprise_data.csv">Download CSV</a>'

@app.route('/enterprise_data.csv')
def serve_csv():
    return send_file(CSV_FILE)

def start_server():
    app.run(host='0.0.0.0', port=10001)  # ← New port

# --- Main Program ---
if __name__ == "__main__":
    threading.Thread(target=start_server, daemon=True).start()
    print("Starting enterprise logger...")
    while True:
        log_data()
        time.sleep(60)
