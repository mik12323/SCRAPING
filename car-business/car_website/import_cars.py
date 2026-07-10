import os
import re
import sqlite3

if os.name == 'posix':  # PythonAnywhere
    DB_PATH = "/home/saucecar/users.db"
    DIST_DIR = "/home/saucecar/carsite/dist"
else:  # Windows local
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")
    DIST_DIR = r"D:\carsfsale\dist"
    if not os.path.exists(DIST_DIR):
        DIST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dist")

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

def parse_price(price_str):
    price_str = price_str.lower().strip()
    if price_str.endswith('m'):
        return int(float(price_str[:-1]) * 1000000)
    elif price_str.endswith('k'):
        return int(float(price_str[:-1]) * 1000)
    return int(price_str)

def parse_folder(folder_name):
    # Handle optional "Cash" prefix in folder names
    pattern = r'^(?:Cash\s+)?(\w+)\s+(.+?)\s+(\d{4})\s+([\d.]+[mk])$'
    match = re.match(pattern, folder_name, re.IGNORECASE)
    if match:
        brand = match.group(1)
        model = match.group(2).strip()
        year = int(match.group(3))
        price = parse_price(match.group(4))
        return brand, model, year, price
    return None

imported = 0
skipped = 0
duplicate = 0

for folder in sorted(os.listdir(DIST_DIR)):
    folder_path = os.path.join(DIST_DIR, folder)
    if os.path.isdir(folder_path):
        c.execute("SELECT id FROM listings WHERE folder_name = ?", (folder,))
        if c.fetchone():
            print(f"Duplicate, skipping: {folder}")
            duplicate += 1
            continue

        result = parse_folder(folder)
        if result:
            brand, model, year, price = result
            photos = [f for f in os.listdir(folder_path) if f.lower().endswith(('.jpg', '.png', '.jpeg'))]
            photo_paths = ','.join(photos)
            location = 'Philippines'
            
            # Read description from details.txt if it exists
            description = ''
            details_file = os.path.join(folder_path, "details.txt")
            if os.path.exists(details_file):
                with open(details_file, 'r', encoding="utf-8", errors="ignore") as f:
                    description = f.read().strip()
            
            c.execute("""
                INSERT INTO listings 
                (user_id, brand, model, year, price, car_type, body_type, fuel_type, transmission, 
                 owner_phone, location, description, photo_paths, folder_name, status)
                VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, 'approved')
            """, (1, brand, model, year, price, 'Used', '+639970946623', location, description, photo_paths, folder))
            
            print(f"Imported: {brand} {model} {year} - P{price:,}")
            imported += 1
        else:
            print(f"Skipped (couldn't parse): {folder}")
            skipped += 1

conn.commit()
conn.close()
print(f"\nDone! Imported: {imported}, Duplicates: {duplicate}, Skipped: {skipped}")
