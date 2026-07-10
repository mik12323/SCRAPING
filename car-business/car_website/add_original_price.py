import sqlite3
import os

if os.name == 'posix':  # PythonAnywhere
    DB_PATH = "/home/saucecar/users.db"
else:  # Windows local
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute("PRAGMA table_info(listings)")
cols = [row[1] for row in c.fetchall()]

if 'original_price' not in cols:
    print("Adding original_price column...")
    c.execute("ALTER TABLE listings ADD COLUMN original_price INTEGER")
    c.execute("UPDATE listings SET original_price = price")
    conn.commit()
    print("Migration complete: original_price column added and initialized.")
else:
    print("Column original_price already exists.")

conn.close()
