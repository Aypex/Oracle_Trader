# refiner.py - The Secure Strategy Refinement and Backtesting Engine

import os
import json
import random
import pandas as pd
import psycopg2  # Use the secure PostgreSQL driver

# Import the core trading algorithm from the strategy module
from strategy import ranked_momentum_rotation_strategy

# --- Secure Configuration ---
# The database URL is read from the environment variables set in Railway.
DATABASE_URL = os.environ.get('DATABASE_URL')
NUM_COUNCIL_MEMBERS = 5

# --- Secure Database Helper Functions ---

def _get_db_connection():
    """Establishes a secure connection to the PostgreSQL database."""
    if not DATABASE_URL:
        raise ValueError("FATAL: DATABASE_URL environment variable is not set. Please set it in Railway.")
    return psycopg2.connect(DATABASE_URL)

def _log_refiner_event(event_type, content_dict):
    """A local logging function for the refiner to report its status to the central events table."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    # Use json.dumps to convert the Python dict to a JSON string for the JSONB column
    cursor.execute("INSERT INTO events (type, content) VALUES (%s, %s)", (event_type, json.dumps(content_dict)))
    conn.commit()
    cursor.close()
    conn.close()

# --- Core Backtesting Function ---

def backtest_strategy(historical_data_df, rules):
    """
    Performs a backtest by running the ranked_momentum_rotation_strategy
    and then calculating a performance score from its signals.
    """
    if historical_data_df is None or historical_data_df.empty:
        return 0 # Cannot backtest without data; return a neutral score.

    # 1. Generate the trading signals using the strategy module
    signals = ranked_momentum_rotation_strategy(
        df=historical_data_df,
        trend_window=rules['trend_window'],
        momentum_window=rules['momentum_window']
    )
    
    # 2. Calculate performance based on the signals.
    # This simplified model calculates total cumulative returns.
    price_cols = historical_data_df.filter(like='_price').columns
    returns_df = historical_data_df[price_cols].pct_change()
    held_asset_signal = signals.shift(1) # Shift to trade on the next period's open
    portfolio_returns = pd.Series(index=returns_df.index, data=0.0)

    for day, signal in held_asset_signal.dropna().items():
        if signal == 'hold_usdt':
            portfolio_returns[day] = 0
        else:
            asset_to_hold = signal.split('_')[1]
            price_col = f"{asset_to_hold}_price"
            if price_col in returns_df.columns:
                portfolio_returns[day] = returns_df.loc[day, price_col]

    # The score is the final value of a $1 investment.
    score = (1 + portfolio_returns).cumprod().iloc[-1]
    return score

# --- The Council of Oracles Functions ---

def nominate_council_from_hall_of_fame():
    """Selects the top N configurations from the database to serve on the Shadow Council."""
    print("Refiner: Nominating the Shadow Council...")
    conn = _get_db_connection()
    try:
        # Read configurations directly into a pandas DataFrame
        query = "SELECT * FROM configurations ORDER BY (backtest_score + shadow_score) DESC"
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Refiner: Could not read configurations, likely empty. Error: {e}")
        df = pd.DataFrame() # Return an empty DataFrame on error or if table is empty
    finally:
        conn.close()
    
    council = df.head(NUM_COUNCIL_MEMBERS).to_dict('records')
    print(f"Refiner: Council selected with {len(council)} members.")
    return council

def run_shadow_simulation(shadow_council, recent_market_data_df):
    """Runs the Shadow Council against recent data to see how they would have performed."""
    print("Refiner: Running Shadow Arena simulation...")
    for member in shadow_council:
        member['latest_performance'] = backtest_strategy(recent_market_data_df, member)
        
        conn = _get_db_connection()
        cursor = conn.cursor()
        new_shadow_score = member.get('shadow_score', 0) + member.get('latest_performance', 0)
        # Use UPDATE with a WHERE clause to target the specific configuration
        cursor.execute("UPDATE configurations SET shadow_score = %s WHERE id = %s", (new_shadow_score, member['id']))
        conn.commit()
        cursor.close()
        conn.close()
    print("Refiner: Shadow simulation complete.")
    return shadow_council

def find_new_champion(historical_data_df, recent_market_data_df, old_rules):
    """The main public function of this module. Runs the full refinement process."""
    _log_refiner_event('REFINER_STATUS', {'status': 'Refinement process started.'})
    
    print("Refiner: Searching for a new challenger...")
    # This is a simple random search. More advanced methods could be used here.
    new_challenger = {
        'trend_window': random.randint(20, 100),
        'momentum_window': random.randint(10, 50),
        'id': 'challenger' # A special ID to distinguish it from database entries
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
        # Insert the new champion into the Hall of Fame
        cursor.execute(
            "INSERT INTO configurations (trend_window, momentum_window, backtest_score) VALUES (%s, %s, %s)",
            (winner['trend_window'], winner['momentum_window'], winner['backtest_score'])
        )
        conn.commit()
        cursor.close()
        conn.close()
        print("Refiner: New challenger has been inducted into the Hall of Fame.")
    
    _log_refiner_event('REFINER_STATUS', {'status': 'Refinement process finished.', 'winner_id': str(winner.get('id'))})
    return new_best_params, winner
