# trader.py - The Secure, Live Trading Operator (with Dynamic Intervals)

import time
import os
import json
import pandas as pd
import psycopg2  # The library for connecting to PostgreSQL

# --- Import Custom Modules ---
import refiner
import finance_manager

# --- Secure Configuration from Environment Variables ---
DATABASE_URL = os.environ.get('DATABASE_URL')

# --- Bot Configuration ---
HISTORICAL_DATA_CSV = 'btc_historical_data.csv'
RECENT_DATA_PERIOD = 30

# --- Tutoring Knowledge Base ---
TUTORING_KNOWLEDGE_BASE = {
    "promotion": "Insight: A new challenger strategy has outperformed the council in recent simulations. Promoting it to be the live strategy.",
    "reaffirmation": "Insight: The current live strategy is still the top performer against recent market data. Reaffirming its rules.",
}


# --- Secure Database Functions (PostgreSQL) ---

def _get_db_connection():
    """Establishes a secure connection to the PostgreSQL database."""
    if not DATABASE_URL:
        raise ValueError("FATAL: DATABASE_URL environment variable is not set. Please set it in Railway.")
    return psycopg2.connect(DATABASE_URL)

def _get_setting(key, default):
    """Securely reads a single setting from the key_value_store table."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM key_value_store WHERE key = %s", (key,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    # Return the integer value of the setting if found, otherwise the default
    return int(result[0]) if result else default

def validate_and_initialize_database():
    """Initializes all necessary tables in the production database."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, timestamp TIMESTAMPTZ DEFAULT NOW(), type TEXT, content JSONB);''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS configurations (
            id SERIAL PRIMARY KEY, timestamp TIMESTAMPTZ DEFAULT NOW(),
            trend_window INTEGER, momentum_window INTEGER,
            backtest_score REAL, shadow_score REAL DEFAULT 0
        );
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS key_value_store (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    ''')
    print("Database is correct and all tables are ready.")
    conn.commit()
    cursor.close()
    conn.close()

def log_event(event_type, content_dict):
    """Logs an event to the database for the dashboard to read."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO events (type, content) VALUES (%s, %s)", (event_type, json.dumps(content_dict)))
    conn.commit()
    cursor.close()
    conn.close()


# --- Core Bot Logic ---

def run_refinement_and_tutoring_process(historical_data, recent_data, old_rules):
    """Delegates to the refiner and logs the outcome."""
    log_event("STATUS", {"message": "Handing off to the refiner module."})
    
    new_best_params, winner = refiner.find_new_champion(historical_data, recent_data, old_rules)

    if new_best_params != old_rules:
        tutoring_insight = TUTORING_KNOWLEDGE_BASE['promotion']
    else:
        tutoring_insight = TUTORING_KNOWLEDGE_BASE['reaffirmation']
    
    print(tutoring_insight)
    insight_content = {
        "insight": tutoring_insight,
        "promoted_rules": new_best_params,
        "old_rules": old_rules,
        "winner_id": str(winner.get('id')),
        "winner_performance": winner.get('latest_performance')
    }
    log_event("INSIGHT", insight_content)

    with open('rules.json', 'w') as f:
        json.dump(new_best_params, f)
    print(f"rules.json has been updated. New leader: {new_best_params}")
    return new_best_params


# --- Main Application Loop ---

def main():
    print("Oracle Trader Council Bot is initializing...")
    validate_and_initialize_database()

    print(f"Loading historical data from {HISTORICAL_DATA_CSV}...")
    try:
        full_historical_data = pd.read_csv(HISTORICAL_DATA_CSV, index_col='timestamp', parse_dates=True)
        print("Historical data loaded successfully.")
    except FileNotFoundError:
        print(f"FATAL ERROR: The data file '{HISTORICAL_DATA_CSV}' was not found.")
        log_event("ERROR", {"message": f"Historical data file not found: {HISTORICAL_DATA_CSV}"})
        return

    if not os.path.exists('rules.json'):
        initial_rules = {'trend_window': 50, 'momentum_window': 20}
        with open('rules.json', 'w') as f: json.dump(initial_rules, f)
    
    with open('rules.json', 'r') as f: current_rules = json.load(f)

    cycle_count = 0

    print("Oracle Trader is running.")
    log_event("STATUS", {"message": "Bot startup complete. Entering main loop."})

    while True:
        try:
            # --- DYNAMICALLY READ SETTINGS FROM DATABASE AT THE START OF EACH CYCLE ---
            # Default to 12 cycles (12 hours) for the "Standard" preset if not set in DB
            refinement_interval = _get_setting('refinement_interval_setting', 12)

            cycle_count += 1
            print(f"\n--- New Cycle #{cycle_count} (Refinement every {refinement_interval} cycles) ---")
            log_event("STATUS", {"message": f"Cycle #{cycle_count} started."})

            # 1. Handle financial tasks
            finance_manager.check_and_process_withdrawal()
            
            # 2. Live trading logic placeholder
            log_event("TRADE", {"action": "simulated_trade_check", "rules": current_rules})

            # 3. Check if it's time to refine the strategy
            if cycle_count % refinement_interval == 0:
                print("Refinement interval reached.")
                recent_data = full_historical_data.tail(RECENT_DATA_PERIOD)
                
                current_rules = run_refinement_and_tutoring_process(
                    full_historical_data,
                    recent_data,
                    current_rules
                )

        except Exception as e:
            print(f"FATAL ERROR occurred in the main trader loop: {e}")
            log_event("ERROR", {"message": str(e)})
        
        print("Cycle complete. Sleeping for 1 hour.")
        log_event("STATUS", {"message": "Cycle complete. Entering 1-hour sleep."})
        time.sleep(3600)

if __name__ == "__main__":
    main()
