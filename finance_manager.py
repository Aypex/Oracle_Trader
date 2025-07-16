# finance_manager.py - The Secure, Interactive Financial Operations Module

import os
import json
import random
from datetime import date
import psycopg2  # Use the secure PostgreSQL driver

# --- Secure Configuration from Environment Variables ---
DATABASE_URL = os.environ.get('DATABASE_URL')
EXCHANGE_API_KEY = os.environ.get('EXCHANGE_API_KEY')
EXCHANGE_SECRET_KEY = os.environ.get('EXCHANGE_SECRET_KEY')

# --- Financial Policy Configuration ---
PROFITS_WITHDRAWAL_PERCENTAGE = 0.50  # 50% of new profits
WITHDRAWAL_DAYS = [1, 15]  # Days of the month to process withdrawals
DEFAULT_WITHDRAWAL_CURRENCY = 'btc'  # Default if user makes no selection

# --- Secure Database Helper Functions ---

def _get_db_connection():
    """Establishes a secure connection to the PostgreSQL database."""
    if not DATABASE_URL:
        raise ValueError("FATAL: DATABASE_URL environment variable is not set.")
    return psycopg2.connect(DATABASE_URL)

def _get_finance_value(key, default=None):
    """Retrieves a single value from the key-value store."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM key_value_store WHERE key = %s", (key,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else default

def _set_finance_value(key, value):
    """Saves or updates a value in the key-value store."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    # Use ON CONFLICT to handle both INSERT and UPDATE scenarios cleanly
    cursor.execute("""
        INSERT INTO key_value_store (key, value) VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
    """, (key, str(value)))
    conn.commit()
    cursor.close()
    conn.close()
    
def _clear_finance_value(key):
    """Removes a key from the key-value store."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM key_value_store WHERE key = %s", (key,))
    conn.commit()
    cursor.close()
    conn.close()

def _log_finance_event(content_dict):
    """Logs a financial event to the central events table."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO events (type, content) VALUES (%s, %s)", ('FINANCE', json.dumps(content_dict)))
    conn.commit()
    cursor.close()
    conn.close()

# --- CRITICAL PLACEHOLDER API FUNCTIONS ---
# These functions MUST be replaced with real exchange API calls.

def _get_current_account_value():
    """
    *** CRITICAL PLACEHOLDER ***
    This function must be replaced with an authenticated API call to your
    exchange to get the current total portfolio value in USD.
    It should use the EXCHANGE_API_KEY and EXCHANGE_SECRET_KEY.
    """
    # Example:
    # from your_exchange_library import Client
    # client = Client(EXCHANGE_API_KEY, EXCHANGE_SECRET_KEY)
    # return client.get_portfolio_value_usd()

    # For simulation, we'll pretend the value fluctuates.
    last_val = float(_get_finance_value('simulated_account_value', 10000.0))
    new_val = last_val + random.uniform(-150.0, 150.0)
    _set_finance_value('simulated_account_value', new_val)
    return new_val

def _get_current_asset_price(asset_symbol):
    """
    *** CRITICAL PLACEHOLDER ***
    This function must be replaced with a real-time API call to your exchange
    to get the current price of an asset (e.g., 'btc' -> 65000.0).
    """
    # Example: client.get_ticker_price(f'{asset_symbol.upper()}USDT')
    if asset_symbol.lower() == 'btc': return 65000.0 + random.uniform(-100, 100)
    if asset_symbol.lower() == 'eth': return 3500.0 + random.uniform(-50, 50)
    return 1.0

def _execute_withdrawal(amount_crypto, currency, address):
    """
    *** CRITICAL PLACEHOLDER ***
    This function must be replaced with an authenticated API call to your
    exchange to execute a withdrawal of a specific crypto amount to a specific address.
    """
    print(f"FINANCE MANAGER: Executing simulated SECURE withdrawal of {amount_crypto:.8f} {currency.upper()} to address {address}")
    # Example: client.withdraw(asset=currency.upper(), amount=amount_crypto, address=address)
    # Ensure you handle API errors and confirmations here.


# --- Core Financial Logic ---

def process_pending_withdrawal():
    """Checks for and processes a withdrawal that was previously pended due to a missing address."""
    pending_usd_str = _get_finance_value('pending_withdrawal_amount_usd')
    if not pending_usd_str:
        return # Nothing to process

    withdrawal_address = _get_finance_value('user_withdrawal_address')
    if not withdrawal_address:
        return # Address still not provided

    print("FINANCE MANAGER: Address found for a pending withdrawal. Processing now.")
    pending_usd = float(pending_usd_str)
    currency = _get_finance_value('pending_withdrawal_currency')
    price = _get_current_asset_price(currency)
    
    amount_in_crypto = pending_usd / price
    
    _execute_withdrawal(amount_in_crypto, currency, withdrawal_address)
    
    _log_finance_event({
        "status": "Executed pending withdrawal.", "pending_usd_amount": pending_usd,
        "currency": currency, "price_at_execution": price, "crypto_amount": amount_in_crypto,
        "address": withdrawal_address
    })
    
    # Clear the pending withdrawal keys now that it's been processed
    _clear_finance_value('pending_withdrawal_amount_usd')
    _clear_finance_value('pending_withdrawal_currency')

def check_and_process_withdrawal():
    """
    The main public function. This should be called on every cycle by trader.py.
    It first tries to process any pending withdrawals, then checks if a new one is due.
    """
    # Always check if we can clear a pending withdrawal first.
    process_pending_withdrawal()
    
    # Now, check if it's a scheduled day for a NEW withdrawal.
    today = date.today()
    if today.day not in WITHDRAWAL_DAYS:
        return

    last_check_date_str = _get_finance_value('last_withdrawal_check')
    if last_check_date_str == str(today):
        return # Already ran the check for a new withdrawal today.

    print("FINANCE MANAGER: It's a scheduled day. Running new withdrawal check...")
    _log_finance_event({"status": "Scheduled new withdrawal check started."})

    account_value = _get_current_account_value()
    # On first run, HWM is the current account value.
    high_water_mark = float(_get_finance_value('high_water_mark', account_value))

    if account_value > high_water_mark:
        profit = account_value - high_water_mark
        withdrawal_amount_usd = profit * PROFITS_WITHDRAWAL_PERCENTAGE
        
        # Read user preferences from the database (set by the dashboard)
        currency = _get_finance_value('user_withdrawal_currency', default=DEFAULT_WITHDRAWAL_CURRENCY)
        withdrawal_address = _get_finance_value('user_withdrawal_address')

        # Update High Water Mark immediately, as profit has been realized.
        _set_finance_value('high_water_mark', account_value)

        if withdrawal_address:
            # Address exists, proceed with immediate withdrawal.
            price = _get_current_asset_price(currency)
            amount_in_crypto = withdrawal_amount_usd / price
            _execute_withdrawal(amount_in_crypto, currency, withdrawal_address)
            _log_finance_event({
                "status": "Profit withdrawal processed.", "amount_usd": withdrawal_amount_usd,
                "currency": currency, "crypto_amount": amount_in_crypto, "address": withdrawal_address,
                "new_hwm": account_value
            })
        else:
            # No address, create a pending withdrawal.
            _set_finance_value('pending_withdrawal_amount_usd', withdrawal_amount_usd)
            _set_finance_value('pending_withdrawal_currency', currency)
            print(f"FINANCE MANAGER: No withdrawal address found. Pended ${withdrawal_amount_usd:.2f} for future withdrawal.")
            _log_finance_event({
                "status": "Withdrawal pended (address missing).", "amount_usd": withdrawal_amount_usd,
                "currency": currency, "new_hwm": account_value
            })
    else:
        # No new profit.
        _log_finance_event({"status": "Check complete. No new profit above HWM.", "account_value": account_value, "current_hwm": high_water_mark})

    # Record that the check was performed today to prevent multiple runs.
    _set_finance_value('last_withdrawal_check', str(today))
    print("FINANCE MANAGER: Check complete.")
