import sqlite3
import os
from werkzeug.security import generate_password_hash

if os.name == 'posix':  # PythonAnywhere
    DB_PATH = "/home/saucecar/users.db"
else:  # Windows local
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("""CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    phone TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    is_admin BOOLEAN DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)""")

c.execute("""CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    brand TEXT NOT NULL,
    model TEXT NOT NULL,
    year INTEGER NOT NULL,
    price INTEGER NOT NULL,
    original_price INTEGER,
    car_type TEXT NOT NULL,
    body_type TEXT,
    fuel_type TEXT,
    transmission TEXT,
    owner_phone TEXT NOT NULL,
    location TEXT NOT NULL,
    description TEXT,
    photo_paths TEXT,
    folder_name TEXT,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
)""")

# Check if admin user exists
c.execute("SELECT id FROM users WHERE username = ?", ("admin",))
if not c.fetchone():
    pw = generate_password_hash("Newuser123123!!!")
    c.execute(
        "INSERT INTO users (username, phone, password_hash, is_admin) VALUES (?, ?, ?, 1)",
        ("admin", "+639970946623", pw)
    )
    print("Admin user created: admin / Newuser123123!!!")
else:
    print("Admin user already exists - skipping...")

# Add missing columns if they don't exist
c.execute("PRAGMA table_info(listings)")
cols = [row[1] for row in c.fetchall()]

if 'body_type' not in cols:
    print("Adding body_type column...")
    c.execute("ALTER TABLE listings ADD COLUMN body_type TEXT")
if 'fuel_type' not in cols:
    print("Adding fuel_type column...")
    c.execute("ALTER TABLE listings ADD COLUMN fuel_type TEXT")
if 'transmission' not in cols:
    print("Adding transmission column...")
    c.execute("ALTER TABLE listings ADD COLUMN transmission TEXT")
if 'original_price' not in cols:
    print("Adding original_price column...")
    c.execute("ALTER TABLE listings ADD COLUMN original_price INTEGER")
    c.execute("UPDATE listings SET original_price = price")

conn.commit()
conn.close()
print("Database initialized successfully!")
