import sqlite3
import os
from werkzeug.security import generate_password_hash

if os.name == 'posix':  # PythonAnywhere
    DB_PATH = "/home/saucecar/users.db"
else:  # Windows local
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")

admin_username = input("Enter admin username (default: admin): ").strip() or "admin"
admin_phone = input("Enter admin phone number (default: +639970946623): ").strip() or "+639970946623"
admin_password = input("Enter admin password: ").strip()

if not admin_password:
    print("Password cannot be empty.")
    exit(1)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        phone TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        is_admin BOOLEAN DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        brand TEXT NOT NULL,
        model TEXT NOT NULL,
        year INTEGER NOT NULL,
        price INTEGER NOT NULL,
        car_type TEXT NOT NULL,
        owner_phone TEXT NOT NULL,
        location TEXT NOT NULL,
        description TEXT,
        photo_paths TEXT,
        folder_name TEXT,
        status TEXT DEFAULT 'pending',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
""")

password_hash = generate_password_hash(admin_password)
try:
    cursor.execute(
        "INSERT INTO users (username, phone, password_hash, is_admin) VALUES (?, ?, ?, 1)",
        (admin_username, admin_phone, password_hash)
    )
    conn.commit()
    print(f"Admin user '{admin_username}' created successfully.")
except sqlite3.IntegrityError:
    print(f"User '{admin_username}' already exists.")

conn.close()
print(f"Database created at: {DB_PATH}")
print("Setup complete. You can now run: python app.py")
