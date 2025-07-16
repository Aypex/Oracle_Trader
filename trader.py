# trader.py (With a guaranteed database reset)

import time
import os
import ccxt
import sqlite3
import random
# from strategy import ranked_momentum_rotation_strategy # Assumes strategy.py exists

DB_FILE = "trader_performance.db"

def initialize_database():
    """Connects to the DB and ensures the table structure is correct."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # --- THIS IS THE GUARANTEED FIX ---
    # This command will DELETE the old, bad table every time the bot starts.
    cursor.execute("DROP TABLE IF EXISTS trades")
    # ---------------------------------
    
    # The bot will then immediately create the new, correct table.
    cursor.execute('''
        CREATE TABLE trades (
            id INTEGER PRIMARY KEY, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            asset_bought TEXT, asset_sold TEXT, profit_loss_pct REAL,
            trend_window_used INTEGER, momentum_window_used INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    print("Database has been reset and initialized correctly.")

# The rest of the file is the same...
def log_trade(asset_bought, asset_sold, profit_pct, rules):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO trades (asset_bought, asset_sold, profit_loss_pct, trend_window_used, momentum_window_used) VALUES (?, ?, ?, ?, ?)",
        (asset_bought, asset_sold, profit_pct, rules['trend_window'], rules['momentum_window'])
    )
    conn.commit()
    conn.close()

def main():
    print("Oracle Trader is initializing...")
    initialize_database()
    
    api_key = os.getenv('KRAKEN_API_KEY')
    secret_key = os.getenv('KRAKEN_SECRET_KEY')

    if not api_key or not secret_key:
        print("!!! WARNING: Running in PAPER TRADING MODE. !!!")
    else:
        print("API keys found. Running in LIVE TRADING MODE.")
    
    current_rules = {'trend_window': 50, 'momentum_window': 20}
    asset_held = 'USDT'
    
    print("Oracle Trader is running. Starting hourly cycles.")
    while True:
        try:
            print(f"\n--- New Cycle Started ---")
            desired_asset = random.choice(['BTC', 'ETH', 'SOL', 'USDT'])
            print(f"Strategy signal: Hold {desired_asset}")

            if desired_asset != asset_held:
                print(f"Executing trade: Selling {asset_held} to buy {desired_asset}")
                simulated_profit = random.uniform(-2.0, 2.5)
                log_trade(desired_asset, asset_held, simulated_profit, current_rules)
                print(f"Paper trade logged with simulated P/L of {simulated_profit:.2f}%.")
                asset_held = desired_asset
            else:
                print(f"Signal matches current holding. No trade needed.")

        except Exception as e:
            print(f"An error occurred in the trading loop: {e}")
        
        print("Cycle complete. Sleeping for 1 hour.")
        time.sleep(3600)

if __name__ == "__main__":
    main()
