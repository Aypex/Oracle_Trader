# create_first_user.py - A one-time script to create the initial admin user.

import os
import psycopg2
from werkzeug.security import generate_password_hash

# --- Configuration ---
# The script will use this username by default. You can change it if you wish.
NEW_USERNAME = "admin"

# Set your desired password here.
NEW_PASSWORD = "securingmyfuturethruai"  # <<<--- CHANGE THIS LINE

# --- Database Connection ---
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("""
        FATAL: DATABASE_URL environment variable is not set.
        You must run this script in an environment where it can connect to the database.
        In Railway, run it from the 'Shell' tab of your application service after deploying.
    """)

def _get_db_connection():
    """Establishes a secure connection to the PostgreSQL database."""
    return psycopg2.connect(DATABASE_URL)

def create_user(username, password):
    """Creates the user table and inserts the new user."""
    if password == "a_very_strong_password_change_this":
        print("!!! WARNING: You are using the default password.")
        print("!!! Please edit this script and set a secure password before running.")
        return

    conn = _get_db_connection()
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        );
    """)

    # Check if the user already exists
    cursor.execute("SELECT id FROM users WHERE username = %s", (username,))
    if cursor.fetchone():
        print(f"User '{username}' already exists. No action taken.")
        conn.close()
        return

    # Hash the password and insert the new user
    password_hash = generate_password_hash(password)
    cursor.execute("INSERT INTO users (username, password_hash) VALUES (%s, %s)", (username, password_hash))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Successfully created user '{username}'.")
    print("You can now log in with this username and your chosen password.")
    print("For security, you may want to delete this script now or remove the password from it.")

if __name__ == '__main__':
    print("--- Initial User Setup Script ---")
    create_user(NEW_USERNAME, NEW_PASSWORD)
