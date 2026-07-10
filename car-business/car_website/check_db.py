import sqlite3
import os

if os.name == 'posix':
    DB_PATH = "/home/saucecar/users.db"
else:
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
c = conn.cursor()

# List all tables
print("=== TABLES ===")
c.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = c.fetchall()
for t in tables:
    print(f"  {t['name']}")

# Check if listings table exists
print("\n=== LISTINGS SCHEMA ===")
c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='listings'")
if not c.fetchone():
    print("ERROR: listings table does NOT exist!")
    conn.close()
    exit(1)

c.execute("PRAGMA table_info(listings)")
cols = c.fetchall()
existing_cols = []
for col in cols:
    nullable = "NOT NULL" if col['notnull'] else "nullable"
    print(f"  {col['name']}: {col['type']} ({nullable})")
    existing_cols.append(col['name'])

# Check for missing columns
required_cols = ['body_type', 'fuel_type', 'transmission']
missing = [col for col in required_cols if col not in existing_cols]
if missing:
    print(f"\nMISSING columns: {missing}")
else:
    print(f"\nAll required columns exist: {required_cols}")

# Sample data
print("\n=== SAMPLE DATA (first 3 rows) ===")
c.execute("SELECT id, brand, model, year, body_type, fuel_type, transmission FROM listings LIMIT 3")
rows = c.fetchall()
for row in rows:
    d = dict(row)
    print(f"  ID {d.get('id')}: brand={d.get('brand')}, model={d.get('model')}, year={d.get('year')}")
    print(f"    body_type={d.get('body_type')}, fuel_type={d.get('fuel_type')}, transmission={d.get('transmission')}")

conn.close()
print("\nDone!")
