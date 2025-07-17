# dashboard.py - The Secure Flask Web Dashboard

import os
import json
from flask import Flask, render_template, redirect, url_for, request, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash
import psycopg2

# --- App Configuration & Login Manager Setup ---
app = Flask(__name__)
SECRET_KEY = os.environ.get('FLASK_SECRET_KEY', 'a-very-secret-key-for-development')
app.config['SECRET_KEY'] = SECRET_KEY
DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("FATAL: DATABASE_URL environment variable is not set.")
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# --- User Model & Database Functions ---
class User(UserMixin):
    def __init__(self, id, username, password_hash):
        self.id = id
        self.username = username
        self.password_hash = password_hash

def _get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def _set_db_value(key, value):
    """Helper function to set a key in the key_value_store table."""
    conn = _get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO key_value_store (key, value) VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
    """, (key, str(value)))
    conn.commit()
    cursor.close()
    conn.close()

@login_manager.user_loader
def load_user(user_id):
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
    if current_user.is_authenticated:
        return redirect(url_for('protected_dashboard'))
    if request.method == 'POST':
        # ... (Login logic remains the same)
        username, password = request.form['username'], request.form['password']
        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, password_hash FROM users WHERE username = %s", (username,))
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()
        if user_data and check_password_hash(user_data[2], password):
            user = User(id=user_data[0], username=user_data[1], password_hash=user_data[2])
            login_user(user, remember=True)
            return redirect(url_for('protected_dashboard'))
        else:
            flash('Invalid username or password.')
    return render_template('login.html')

@app.route('/dashboard')
@login_required
def protected_dashboard():
    bot_status = "Waiting for status..."
    live_rules = {"trend_window": "N/A", "momentum_window": "N/A"}
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT content FROM events WHERE type = 'STATUS' ORDER BY timestamp DESC LIMIT 1")
        latest_status_event = cursor.fetchone()
        if latest_status_event:
            bot_status = latest_status_event[0].get('message', 'Could not parse status.')
        cursor.execute("SELECT content FROM events WHERE type = 'INSIGHT' ORDER BY timestamp DESC LIMIT 1")
        latest_insight_event = cursor.fetchone()
        if latest_insight_event:
            live_rules = latest_insight_event[0].get('promoted_rules', live_rules)
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error fetching data from database: {e}")
        bot_status = "Error connecting to database."
    return render_template('protected_dashboard.html', username=current_user.username, bot_status=bot_status, live_rules=live_rules)

# --- NEW ACTION ROUTE FOR THE BUTTON ---
@app.route('/force-refinement', methods=['POST'])
@login_required
def force_refinement():
    """Sets a flag in the database to tell the bot to run refinement on its next cycle."""
    try:
        _set_db_value('force_refinement', 'true')
        flash('Refinement signal sent! The bot will refine on its next cycle.', 'success')
    except Exception as e:
        print(f"Error setting force_refinement flag: {e}")
        flash('Error sending refinement signal.', 'error')
    return redirect(url_for('protected_dashboard'))

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/')
def index():
    return redirect(url_for('login'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
