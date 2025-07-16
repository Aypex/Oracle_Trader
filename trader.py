# trader.py (Final Version with Robust Database Logic)

import time
import os
import ccxt
import sqlite3
import random
# This assumes you have a strategy.py file in your repository
# from strategy import ranked_momentum_rotation_strategy

DB_FILE = "trader_performance.db"

def initialize_database():
    """Connects to the DB and creates the table with all necessary columns."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # This is the full, correct table structure.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            asset_bought TEXT,
            asset_sold TEXT,
            profit_loss_pct REAL,
            trend_window_used INTEGER,
            momentum_window_used INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    print("Database initialized successfully.")

def log_trade(asset_bought, asset_sold, profit_pct, rules):
    """Logs a simulated or real trade to the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # This INSERT command now perfectly matches the CREATE TABLE command.
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
        print("!!! WARNING: API keys not found. Running in PAPER TRADING MODE. !!!")
        LIVE_MODE = False
        exchange = ccxt.kraken()
    else:
        print("API keys found. Running in LIVE TRADING MODE.")
        LIVE_MODE = True
        exchange = ccxt.kraken({'apiKey': api_key, 'secret': secret_key})

    current_rules = {'trend_window': 50, 'momentum_window': 20}
    asset_held = 'USDT'
    
    print("Oracle Trader is running. Starting hourly cycles.")
    while True:
        try:
            print(f"\n--- New Cycle Started ---")
            print(f"Current Mode: {'LIVE' if LIVE_MODE else 'PAPER TRADING'}. Currently holding: {asset_held}")
            
            desired_asset = random.choice(['BTC', 'ETH', 'SOL', 'USDT'])
            print(f"Strategy signal: Hold {desired_asset}")

            if desired_asset != asset_held:
                print(f"Executing trade: Selling {asset_held} to buy {desired_asset}")
                
                if LIVE_MODE:
                    print("Executing LIVE trade on Kraken... (logic not yet implemented)")
                else:
                    simulated_profit = random.uniform(-2.0, 2.5)
                    # Correctly call the log_trade function with all required arguments
                    log_trade(desired_asset, asset_held, simulated_profit, current_rules)
                    print(f"Paper trade logged to database with simulated P/L of {simulated_profit:.2f}%.")

                asset_held = desired_asset
            else:
                print(f"Signal matches current holding. No trade needed.")

        except Exception as e:
            print(f"An error occurred in the trading loop: {e}")
        
        print("Cycle complete. Sleeping for 1 hour.")
        time.sleep(3600)

if __name__ == "__main__":
    main()
