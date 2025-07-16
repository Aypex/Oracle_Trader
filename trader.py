# trader.py (Corrected with proper Paper Trading Fallback)

import time
import os
import ccxt
import pandas as pd
import sqlite3
# This assumes you have a strategy.py file in your repository
from strategy import ranked_momentum_rotation_strategy

# --- Database Setup ---
DB_FILE = "trader_performance.db"

def initialize_database():
    """Connects to the DB and creates the table if it doesn't exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            asset_bought TEXT, asset_sold TEXT, profit_loss_pct REAL,
            trend_window_used INTEGER, momentum_window_used INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    print("Database initialized successfully.")

def log_trade(asset_bought, asset_sold, profit_pct, rules):
    """Logs a simulated or real trade to the database."""
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

    # --- Mode Selection (Corrected Logic) ---
    if not api_key or not secret_key:
        print("!!! WARNING: API keys not found. Running in PAPER TRADING MODE. !!!")
        LIVE_MODE = False
        exchange = ccxt.kraken() # Public connection for prices
    else:
        print("API keys found. Running in LIVE TRADING MODE.")
        LIVE_MODE = True
        exchange = ccxt.kraken({'apiKey': api_key, 'secret': secret_key})

    # These would be loaded from rules.json in a full system
    current_rules = {'trend_window': 50, 'momentum_window': 20}
    
    # State tracking for paper trading
    asset_held = 'USDT'
    
    print("Oracle Trader is running. Starting hourly cycles.")
    while True:
        try:
            print(f"\n--- New Cycle Started ---")
            print(f"Current Mode: {'LIVE' if LIVE_MODE else 'PAPER TRADING'}. Currently holding: {asset_held}")
            
            # This is a simplified stand-in for fetching data and running the strategy
            # A real bot would need more complex logic here
            import random
            desired_asset = random.choice(['BTC', 'ETH', 'SOL', 'USDT'])
            print(f"Strategy signal: Hold {desired_asset}")

            if desired_asset != asset_held:
                print(f"Executing trade: Selling {asset_held} to buy {desired_asset}")
                
                if LIVE_MODE:
                    # In live mode, you would place real orders here
                    print("Executing LIVE trade on Kraken... (logic not yet implemented)")
                else:
                    # In paper mode, we just log a simulated result
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
