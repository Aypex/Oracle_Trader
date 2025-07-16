# dashboard.py - The Secure Flask Web Dashboard

import os
from flask import Flask, render_template, redirect, url_for, request, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash, generate_password_hash
import psycopg2

# --- App Configuration ---
app = Flask(__name__)

# This key is required for session management (e.g., "remember me")
# In production, this MUST be a long, random string stored as an environment variable.
SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'a-very-secret-key-for-development')
app.config['SECRET_KEY'] = SECRET_KEY

# Get the database URL from environment variables
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("FATAL: DATABASE_URL environment variable is not set.")

# --- Login Manager Setup ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' # Name of the function that handles the login route

# --- User Model ---
# This class represents a user in our system. It integrates with Flask-Login.
class User(UserMixin):
    def __init__(self, id, username, password_hash):
        self.id = id
        self.username = username
        self.password_hash = password_hash

def _get_db_connection():
    """Establishes a secure connection to the PostgreSQL database."""
    return psycopg2.connect(DATABASE_URL)

def initialize_user_database():
    """Creates the 'users' table if it doesn't exist."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("User database table is ready.")

@login_manager.user_loader
def load_user(user_id):
    """Flask-Login uses this function to reload the user object from the user ID stored in the session."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, password_hash FROM users WHERE id = %s", (int(user_id),))
    user_data = cursor.fetchone()
    cursor.close()
    conn.close()
    if user_data:
        return User(id=user_data[0], username=user_data[1], password_hash=user_data[2])
    return None

# --- Web Page Routes ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Handles the login process."""
    if current_user.is_authenticated:
        return redirect(url_for('protected_dashboard'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, password_hash FROM users WHERE username = %s", (username,))
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()

        # Check if user exists and if the password hash matches
        if user_data and check_password_hash(user_data[2], password):
            user = User(id=user_data[0], username=user_data[1], password_hash=user_data[2])
            login_user(user, remember=True)
            return redirect(url_for('protected_dashboard'))
        else:
            flash('Invalid username or password.')

    return render_template('login.html')

@app.route('/dashboard')
@login_required # This decorator protects the page.
def protected_dashboard():
    """The main dashboard page, visible only to logged-in users."""
    return render_template('protected_dashboard.html', username=current_user.username)

@app.route('/logout')
@login_required
def logout():
    """Logs the current user out."""
    logout_user()
    return redirect(url_for('login'))

@app.route('/')
def index():
    """Redirects the base URL to the login page."""
    return redirect(url_for('login'))

# --- Main Entry Point ---
if __name__ == '__main__':
    print("Starting dashboard web server...")
    initialize_user_database()
    # Railway uses the PORT environment variable to run the web server
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
