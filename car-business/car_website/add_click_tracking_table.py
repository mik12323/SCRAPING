import sqlite3
import os

# Match your existing DB path configuration
if os.name == 'posix':  # PythonAnywhere
    DB_PATH = "/home/saucecar/users.db"
else:  # Windows local
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Only create if table doesn't exist (safe to run multiple times)
c.execute("""
    CREATE TABLE IF NOT EXISTS daily_car_clicks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id INTEGER NOT NULL,
        click_date DATE NOT NULL,
        click_count INTEGER DEFAULT 1,
        UNIQUE(listing_id, click_date),
        FOREIGN KEY (listing_id) REFERENCES listings(id)
    )
""")

conn.commit()

# Verify
c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_car_clicks'")
result = c.fetchone()
print("Success: daily_car_clicks table ready." if result else "Error: Table not created.")
conn.close()
