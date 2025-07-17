# finance_manager.py - The Secure, Interactive Financial Operations Module (with Data Type Fix)

import os
import json
import random
from datetime import date
import psycopg2

# --- Configuration ---
DATABASE_URL = os.environ.get('DATABASE_URL')
EXCHANGE_API_KEY = os.environ.get('EXCHANGE_API_KEY')
EXCHANGE_SECRET_KEY = os.environ.get('EXCHANGE_SECRET_KEY')
PROFITS_WITHDRAWAL_PERCENTAGE = 0.50
WITHDRAWAL_DAYS = [1, 15]
DEFAULT_WITHDRAWAL_CURRENCY = 'btc'

# --- Database Helper Functions ---
def _get_db_connection():
    # ... (This function remains the same)
    if not DATABASE_URL: raise ValueError("FATAL: DATABASE_URL environment variable is not set.")
    return psycopg2.connect(DATABASE_URL)

def _get_finance_value(key, default=None):
    # ... (This function remains the same)
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM key_value_store WHERE key = %s", (key,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else default

def _set_finance_value(key, value):
    # ... (This function remains the same)
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO key_value_store (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;", (key, str(value)))
    conn.commit()
    cursor.close()
    conn.close()

def _clear_finance_value(key):
    # ... (This function remains the same)
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM key_value_store WHERE key = %s", (key,))
    conn.commit()
    cursor.close()
    conn.close()

def _log_finance_event(content_dict):
    # ... (This function remains the same)
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO events (type, content) VALUES (%s, %s)", ('FINANCE', json.dumps(content_dict)))
    conn.commit()
    cursor.close()
    conn.close()

# --- Placeholder API Functions ---
def _get_current_account_value():
    # ... (This function remains the same)
    last_val = float(_get_finance_value('simulated_account_value', 10000.0))
    new_val = last_val + random.uniform(-150.0, 150.0)
    _set_finance_value('simulated_account_value', new_val)
    return new_val

def _get_current_asset_price(asset_symbol):
    # ... (This function remains the same)
    if asset_symbol.lower() == 'btc': return 65000.0 + random.uniform(-100, 100)
    if asset_symbol.lower() == 'eth': return 3500.0 + random.uniform(-50, 50)
    return 1.0

def _execute_withdrawal(amount_crypto, currency, address):
    # ... (This function remains the same)
    print(f"FINANCE MANAGER: Executing simulated SECURE withdrawal of {amount_crypto:.8f} {currency.upper()} to address {address}")


# --- Core Financial Logic (Upgraded with Data Type Conversion) ---
def process_pending_withdrawal():
    # ... (This function remains the same)
    pending_usd_str = _get_finance_value('pending_withdrawal_amount_usd')
    if not pending_usd_str: return
    withdrawal_address = _get_finance_value('user_withdrawal_address')
    if not withdrawal_address: return
    print("FINANCE MANAGER: Address found for a pending user withdrawal. Processing now.")
    pending_usd = float(pending_usd_str)
    currency = _get_finance_value('pending_withdrawal_currency')
    price = _get_current_asset_price(currency)
    amount_in_crypto = pending_usd / price
    _execute_withdrawal(amount_in_crypto, currency, withdrawal_address)
    _log_finance_event({"status": "Executed pending user withdrawal.","pending_usd_amount": pending_usd,"currency": currency,"price_at_execution": price,"crypto_amount": amount_in_crypto,"address": withdrawal_address})
    _clear_finance_value('pending_withdrawal_amount_usd')
    _clear_finance_value('pending_withdrawal_currency')

def check_and_process_withdrawal():
    process_pending_withdrawal()
    today = date.today()
    if today.day not in WITHDRAWAL_DAYS: return
    last_check_date_str = _get_finance_value('last_withdrawal_check')
    if last_check_date_str == str(today): return
    print("FINANCE MANAGER: It's a scheduled day. Running new withdrawal check...")
    _log_finance_event({"status": "Scheduled new withdrawal check started."})
    account_value = _get_current_account_value()
    high_water_mark = float(_get_finance_value('high_water_mark', account_value))

    if account_value > high_water_mark:
        tax_provision_rate = float(_get_finance_value('tax_provision_percentage', 20.0)) / 100.0
        profit_to_distribute = (account_value - high_water_mark) * PROFITS_WITHDRAWAL_PERCENTAGE
        tax_provision_amount = profit_to_distribute * tax_provision_rate
        user_withdrawal_amount_usd = profit_to_distribute * (1 - tax_provision_rate)
        current_total_provision = float(_get_finance_value('total_tax_provision', 0.0))
        new_total_provision = current_total_provision + tax_provision_amount
        
        # --- CONVERSIONS ADDED FOR SAFETY ---
        _set_finance_value('total_tax_provision', float(new_total_provision))
        _set_finance_value('high_water_mark', float(account_value))

        currency = _get_finance_value('user_withdrawal_currency', default=DEFAULT_WITHDRAWAL_CURRENCY)
        withdrawal_address = _get_finance_value('user_withdrawal_address')
        log_details = {"account_value": account_value,"previous_hwm": high_water_mark,"user_withdrawal_usd": user_withdrawal_amount_usd,"tax_provision_usd": tax_provision_amount,"total_tax_provision_so_far": new_total_provision,"new_hwm": account_value}

        if withdrawal_address:
            price = _get_current_asset_price(currency)
            amount_in_crypto = user_withdrawal_amount_usd / price
            _execute_withdrawal(amount_in_crypto, currency, withdrawal_address)
            log_details.update({"status": "Profit withdrawal processed.","currency": currency,"crypto_amount_withdrawn": amount_in_crypto,"address": withdrawal_address})
            _log_finance_event(log_details)
        else:
            _set_finance_value('pending_withdrawal_amount_usd', user_withdrawal_amount_usd)
            _set_finance_value('pending_withdrawal_currency', currency)
            print(f"FINANCE MANAGER: No address. Pended ${user_withdrawal_amount_usd:.2f} for user withdrawal.")
            log_details.update({"status": "User withdrawal pended (address missing)."})
            _log_finance_event(log_details)
    else:
        _log_finance_event({"status": "Check complete. No new profit above HWM.","account_value": account_value,"current_hwm": high_water_mark})

    _set_finance_value('last_withdrawal_check', str(today))
    print("FINANCE MANAGER: Check complete.")
