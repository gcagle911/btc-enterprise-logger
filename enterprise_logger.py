import requests
import csv
import time
import uuid
from datetime import datetime
from flask import Flask, send_file
import threading

CSV_FILE = 'data.csv'
EXCHANGE_NAME = 'Coinbase'
SYMBOL = 'BTC-USD'
LOG_INTERVAL_SECONDS = 5  # Log every 5 seconds

app = Flask(__name__)

# Create CSV with headers if it doesn't exist
def initialize_csv():
    try:
        with open(CSV_FILE, 'x', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'entry_id',
                'timestamp',
                'symbol',
                'exchange',
                'price',
                'bid',
                'ask',
                'mid_price',
                'spread',
                'volume_usd'
            ])
    except FileExistsError:
        pass  # Already exists

# Get BTC-USD ticker from Coinbase API
def fetch_data():
    try:
        response = requests.get('https://api.exchange.coinbase.com/products/BTC-USD/ticker')
        data = response.json()

        bid = float(data['bid'])
        ask = float(data['ask'])
        price = float(data['price'])
        volume = float(data['volume'])  # USD volume
        mid_price = (bid + ask) / 2
        spread = ask - bid

        return {
            'entry_id': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'symbol': SYMBOL,
            'exchange': EXCHANGE_NAME,
            'price': price,
            'bid': bid,
            'ask': ask,
            'mid_price': mid_price,
            'spread': spread,
            'volume_usd': volume
        }

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

# Append row to CSV
def log_data():
    while True:
        entry = fetch_data()
        if entry:
            with open(CSV_FILE, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    entry['entry_id'],
                    entry['timestamp'],
                    entry['symbol'],
                    entry['exchange'],
                    entry['price'],
                    entry['bid'],
                    entry['ask'],
                    entry['mid_price'],
                    entry['spread'],
                    entry['volume_usd']
                ])
        time.sleep(LOG_INTERVAL_SECONDS)

# Serve CSV via Flask
@app.route('/')
def serve_file():
    return send_file(CSV_FILE, mimetype='text/csv')

# Start logging in a separate thread
def start_logger():
    t1 = threading.Thread(target=log_data)
    t1.daemon = True
    t1.start()

    t2 = threading.Thread(target=log_hourly_data)
    t2.daemon = True
    t2.start()

if __name__ == '__main__':
    initialize_csv()
    start_logger()
    app.run(host='0.0.0.0', port=10001)
# Log a clean hourly snapshot
def log_hourly_data():
    last_logged_hour = None
    while True:
        now = datetime.utcnow()
        current_hour = now.strftime('%Y-%m-%d %H')

        if current_hour != last_logged_hour:
            entry = fetch_data()
            if entry:
                with open('hourly_data.csv', 'a', newline='') as f:
                    writer = csv.writer(f)
                    if f.tell() == 0:
                        writer.writerow([
                            'entry_id',
                            'timestamp',
                            'symbol',
                            'exchange',
                            'price',
                            'bid',
                            'ask',
                            'mid_price',
                            'spread',
                            'volume_usd'
                        ])
                    writer.writerow([
                        entry['entry_id'],
                        entry['timestamp'],
                        entry['symbol'],
                        entry['exchange'],
                        entry['price'],
                        entry['bid'],
                        entry['ask'],
                        entry['mid_price'],
                        entry['spread'],
                        entry['volume_usd']
                    ])
            last_logged_hour = current_hour
        time.sleep(30)  # check twice per minute
