# refiner.py - The Secure Strategy Refinement and Backtesting Engine (with Data Type Fix)

import os
import json
import random
import pandas as pd
import psycopg2

from strategy import ranked_momentum_rotation_strategy

# --- Configuration ---
DATABASE_URL = os.environ.get('DATABASE_URL')
EXCHANGE_FEE_PERCENTAGE = float(os.environ.get('EXCHANGE_FEE_PERCENTAGE', 0.1))
NUM_COUNCIL_MEMBERS = 5

# --- Database Helper Functions ---
def _get_db_connection():
    if not DATABASE_URL:
        raise ValueError("FATAL: DATABASE_URL environment variable is not set.")
    return psycopg2.connect(DATABASE_URL)

def _log_refiner_event(event_type, content_dict):
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO events (type, content) VALUES (%s, %s)", (event_type, json.dumps(content_dict)))
    conn.commit()
    cursor.close()
    conn.close()

# --- Core Backtesting Function (Upgraded with Data Type Conversion) ---
def backtest_strategy(historical_data_df, rules):
    if historical_data_df is None or historical_data_df.empty:
        return 0.0 # Return a standard float

    fee_multiplier = EXCHANGE_FEE_PERCENTAGE / 100.0
    signals = ranked_momentum_rotation_strategy(
        df=historical_data_df, trend_window=rules['trend_window'], momentum_window=rules['momentum_window']
    )
    price_cols = historical_data_df.filter(like='_price').columns
    returns_df = historical_data_df[price_cols].pct_change()
    portfolio_value = 1.0
    held_asset = 'usdt'

    for i in range(1, len(historical_data_df)):
        current_signal = signals.iloc[i-1]
        signal_asset = current_signal.split('_')[1]
        if signal_asset != held_asset:
            portfolio_value *= (1 - fee_multiplier)
            held_asset = signal_asset
        if held_asset != 'usdt':
            price_col = f"{held_asset}_price"
            if price_col in returns_df.columns:
                daily_return = returns_df.iloc[i][price_col]
                portfolio_value *= (1 + daily_return)
    
    # The final portfolio value is our score
    score = portfolio_value
    return float(score) # <-- CONVERSION ADDED: Ensure we always return a standard Python float

# --- The Council of Oracles Functions ---
def nominate_council_from_hall_of_fame():
    # ... (This function remains the same)
    print("Refiner: Nominating the Shadow Council...")
    conn = _get_db_connection()
    try:
        query = "SELECT * FROM configurations ORDER BY (backtest_score + shadow_score) DESC"
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Refiner: Could not read configurations, likely empty. Error: {e}")
        df = pd.DataFrame()
    finally:
        conn.close()
    council = df.head(NUM_COUNCIL_MEMBERS).to_dict('records')
    print(f"Refiner: Council selected with {len(council)} members.")
    return council

def run_shadow_simulation(shadow_council, recent_market_data_df):
    # ... (This function remains the same)
    print("Refiner: Running Shadow Arena simulation...")
    for member in shadow_council:
        member['latest_performance'] = backtest_strategy(recent_market_data_df, member)
        conn = _get_db_connection()
        cursor = conn.cursor()
        new_shadow_score = member.get('shadow_score', 0) + member.get('latest_performance', 0)
        cursor.execute("UPDATE configurations SET shadow_score = %s WHERE id = %s", (new_shadow_score, member['id']))
        conn.commit()
        cursor.close()
        conn.close()
    print("Refiner: Shadow simulation complete.")
    return shadow_council

def find_new_champion(historical_data_df, recent_market_data_df, old_rules):
    _log_refiner_event('REFINER_STATUS', {'status': 'Refinement process started.'})
    print("Refiner: Searching for a new challenger...")
    new_challenger = {
        'trend_window': random.randint(20, 100), 'momentum_window': random.randint(10, 50),
        'id': 'challenger'
    }
    new_challenger['latest_performance'] = backtest_strategy(recent_market_data_df, new_challenger)
    new_challenger['backtest_score'] = backtest_strategy(historical_data_df, new_challenger)
    shadow_council = nominate_council_from_hall_of_fame()
    shadow_results = run_shadow_simulation(shadow_council, recent_market_data_df)
    all_candidates = shadow_results + [new_challenger]
    winner = max(all_candidates, key=lambda x: x.get('latest_performance', -999))
    new_best_params = {'trend_window': winner['trend_window'], 'momentum_window': winner['momentum_window']}

    if winner['id'] == 'challenger':
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        # Prepare values for database insertion, ensuring they are standard Python types
        trend_window = int(winner['trend_window']) # <-- CONVERSION ADDED
        momentum_window = int(winner['momentum_window']) # <-- CONVERSION ADDED
        backtest_score = float(winner['backtest_score']) # <-- CONVERSION ADDED

        cursor.execute(
            "INSERT INTO configurations (trend_window, momentum_window, backtest_score) VALUES (%s, %s, %s)",
            (trend_window, momentum_window, backtest_score)
        )
        conn.commit()
        cursor.close()
        conn.close()
        print("Refiner: New challenger has been inducted into the Hall of Fame.")
    
    _log_refiner_event('REFINER_STATUS', {'status': 'Refinement process finished.', 'winner_id': str(winner.get('id'))})
    return new_best_params, winner
