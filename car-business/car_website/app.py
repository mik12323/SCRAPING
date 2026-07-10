from flask import Flask, render_template, request, send_from_directory, redirect, url_for, flash, session
import os
import re
import math
import unicodedata
import sqlite3
import smtplib
import shutil
import random
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

app = Flask(__name__)
from flask import request, redirect

@app.before_request
def force_https():
    # Only force HTTPS in production (not localhost)
    if request.scheme == 'http' and not request.host.startswith('localhost') and not request.host.startswith('127.0.0.1'):
        url = request.url.replace('http://', 'https://', 1)
        return redirect(url, code=301)

@app.before_request
def force_www():
    if request.host == "usedcarsphilippines.com.ph":
        url = request.url.replace("://usedcarsphilippines.com.ph", "://www.usedcarsphilippines.com.ph", 1)
        return redirect(url, code=301)
app.secret_key = os.environ.get("SECRET_KEY", "used-cars-ph-secret-key-change-this-2026")

ALLOWED_EXTENSIONS = {'jpg', 'jpeg', 'png'}
if os.name == 'posix':  # PythonAnywhere
    DB_PATH = "/home/saucecar/users.db"
else:  # Windows local
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")

ADMIN_EMAIL = "koylito3@gmail.com"
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "")

CAR_BRANDS = [
    "Acura", "Alfa Romeo", "Aston Martin", "Audi", "BAIC", "Bentley", "BMW",
    "BYD", "Cadillac", "Changan", "Chery", "Chevrolet", "Chrysler",
    "Daewoo", "Daihatsu","Dodge","Ferrari",
    "Fiat", "Ford", "Foton", "GAC", "Geely", "Genesis", "GMC", "Great Wall", "Haima",
    "Honda", "Hummer", "Hyundai", "Infiniti", "Isuzu", "JAC",
    "Jaguar", "Jeep", "Jetour", "Kia", "Lamborghini", "Land Rover",
    "Lexus", "Lincoln", "Mahindra", "Maserati", "Mazda",
    "McLaren", "Mercedes-Benz", "MG", "Mini", "Mitsubishi", "Nissan",
    "Opel", "Peugeot", "Pontiac", "Porsche", "Proton", "Renault", "Rolls-Royce",
    "SsangYong", "Subaru", "Suzuki", "Tesla",
    "Tata", "Toyota", "VinFast", "Volkswagen", "Volvo"
]

BODY_TYPES = ["Sedan", "SUV", "Hatchback", "Truck", "Van", "Coupe", "Convertible", "Wagon", "Pickup", "Crossover"]
FUEL_TYPES = ["Gas", "Diesel", "Electric", "Hybrid", "Plug-in Hybrid"]
TRANSMISSIONS = ["Automatic", "Manual"]

# CONFIG
if os.path.exists(r"D:\carsfsale\dist"):
    SOURCE_DIR = r"D:\carsfsale\dist"
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    SOURCE_DIR = os.path.join(BASE_DIR, "dist")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def slugify(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text).strip('-_')
    return text

def find_folder_by_slug(slug):
    slug_lower = slug.lower()
    for folder_name in os.listdir(SOURCE_DIR):
        folder_path = os.path.join(SOURCE_DIR, folder_name)
        if os.path.isdir(folder_path) and slugify(folder_name) == slug_lower:
            return folder_name
    return None

def format_price_for_folder(price):
    if price >= 1000000:
        val = price / 1000000
        if val == int(val):
            return f"{int(val)}m"
        return f"{val}m"
    elif price >= 1000:
        val = price / 1000
        if val == int(val):
            return f"{int(val)}k"
        return f"{val}k"
    return str(price)

def format_display_price(price):
    if price >= 1000000:
        price_val = price
        return f"₱{int(price_val):,}"
    elif price >= 1000:
        price_val = price
        return f"₱{int(price_val):,}"
    return "Contact for Price"

# Make utility functions available in templates
app.jinja_env.globals['format_display_price'] = format_display_price

def send_email_notification(brand, model, year, price, username, phone, listing_id):
    if not EMAIL_PASSWORD:
        return
    try:
        msg = MIMEMultipart()
        msg['From'] = ADMIN_EMAIL
        msg['To'] = ADMIN_EMAIL
        msg['Subject'] = f"New Car Listing: {year} {brand} {model} - ₱{price:,}"
        
        body = f"""
New car listing submitted for review:

Car Details:
  Brand: {brand}
  Model: {model}
  Year: {year}
  Price: ₱{price:,}

Poster Information:
  Username: {username}
  Phone: {phone}

Review the listing here:
{url_for('admin_dashboard', _external=True)}

Listing ID: {listing_id}
"""
        msg.attach(MIMEText(body, 'plain'))
        
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(ADMIN_EMAIL, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
    except Exception as e:
        print(f"Email notification failed: {e}")

def get_approved_cars():
    db = get_db()
    listings = db.execute("SELECT * FROM listings WHERE status = 'approved' ORDER BY brand ASC, model ASC").fetchall()
    db.close()

    car_list = []
    for listing in listings:
        price_val = listing['price']
        display_price = format_display_price(price_val)
        folder_name = listing['folder_name']

        images = []
        if folder_name:
            folder_path = os.path.join(SOURCE_DIR, folder_name)
            if os.path.isdir(folder_path):
                images = sorted([f for f in os.listdir(folder_path) if f.lower().endswith(('.jpg', '.png', '.jpeg'))])

        car_list.append({
            "id": listing['id'],
            "folder": folder_name,
            "slug": slugify(folder_name) if folder_name else f"listing-{listing['id']}",
            "type": listing['car_type'],
            "name": f"{listing['brand']} {listing['model']}",
            "brand": listing['brand'],
            "model": listing['model'],
            "year": listing['year'],
            "price": price_val,
            "display_price": display_price,
            "thumbnail": images[0] if images else None,
            "body_type": listing['body_type'] if 'body_type' in listing.keys() else None,
            "fuel_type": listing['fuel_type'] if 'fuel_type' in listing.keys() else None,
            "transmission": listing['transmission'] if 'transmission' in listing.keys() else None,
            "original_price": listing['original_price'] if 'original_price' in listing.keys() else price_val
        })
    return car_list

def track_car_view(listing_id):
    """Increment click count for a listing for today"""
    db = get_db()
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        db.execute("""
            INSERT INTO daily_car_clicks (listing_id, click_date, click_count)
            VALUES (?, ?, 1)
            ON CONFLICT(listing_id, click_date) DO UPDATE SET click_count = click_count + 1
        """, (listing_id, today))
        db.commit()
    except Exception as e:
        print(f"Click tracking failed: {e}")
    finally:
        db.close()

def get_trending_cars(limit=3):
    """Get most clicked cars for today, grouped by base model"""
    db = get_db()
    today = datetime.now().strftime('%Y-%m-%d')
    rows = db.execute("""
        SELECT l.brand, l.model, l.body_type, l.fuel_type, l.transmission,
               COALESCE(dcc.click_count, 0) as clicks
        FROM listings l
        LEFT JOIN daily_car_clicks dcc ON l.id = dcc.listing_id AND dcc.click_date = ?
        WHERE l.status = 'approved' AND COALESCE(dcc.click_count, 0) > 0
    """, (today,)).fetchall()
    db.close()

    # Group by (brand, base_model) and sum clicks
    grouped = {}
    for row in rows:
        model_words = row['model'].split()
        base_model = ' '.join(model_words[:2]) if len(model_words) >= 2 else row['model']
        key = (row['brand'], base_model)

        if key not in grouped:
            grouped[key] = {
                'brand': row['brand'],
                'base_model': base_model,
                'clicks': 0,
                'body_types': [],
                'fuel_types': [],
                'transmissions': []
            }
        grouped[key]['clicks'] += row['clicks']
        if row['body_type']:
            grouped[key]['body_types'].append(row['body_type'])
        if row['fuel_type']:
            grouped[key]['fuel_types'].append(row['fuel_type'])
        if row['transmission']:
            grouped[key]['transmissions'].append(row['transmission'])

    # Filter groups with >= 5 clicks and sort
    filtered = [g for g in grouped.values() if g['clicks'] >= 5]
    sorted_groups = sorted(filtered, key=lambda x: x['clicks'], reverse=True)[:limit]

    result = []
    for g in sorted_groups:
        body_type = max(set(g['body_types']), key=g['body_types'].count) if g['body_types'] else ''
        fuel_type = max(set(g['fuel_types']), key=g['fuel_types'].count) if g['fuel_types'] else ''
        transmission = max(set(g['transmissions']), key=g['transmissions'].count) if g['transmissions'] else ''
        result.append({
            'brand': g['brand'],
            'model': g['base_model'],
            'display_name': f"{g['brand']} {g['base_model']}",
            'clicks': g['clicks'],
            'body_type': body_type,
            'fuel_type': fuel_type,
            'transmission': transmission
        })
    return result


def require_login(f):
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

def require_admin(f):
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session or not session.get('is_admin'):
            flash('Admin access required.', 'danger')
            return redirect('/')
        return f(*args, **kwargs)
    return decorated_function


@app.route('/')
def index():
    return render_template('index.html', trending_cars=get_trending_cars(5))


@app.route('/browse')
def browse_cars():
    cars = get_approved_cars()

    # Make (brand) filter with partial match
    make_filter = request.args.get('make', '').strip()
    if make_filter:
        cars = [c for c in cars if make_filter.lower() in c['brand'].lower()]

    # Model filter (based on selected make)
    model_filter = request.args.get('model', '').strip()
    if model_filter:
        cars = [c for c in cars if model_filter.lower() in c['model'].lower()]

    # Type Filter
    car_type = request.args.get('type', 'All')
    if car_type != 'All':
        cars = [c for c in cars if c['type'] == car_type]

    # Body Type Filter
    body_type = request.args.get('body_type', 'All')
    if body_type != 'All':
        cars = [c for c in cars if c.get('body_type') == body_type]

    # Fuel Type Filter
    fuel_type = request.args.get('fuel_type', 'All')
    if fuel_type != 'All':
        cars = [c for c in cars if c.get('fuel_type') == fuel_type]

    # Transmission Filter
    transmission = request.args.get('transmission', 'All')
    if transmission != 'All':
        cars = [c for c in cars if c.get('transmission') == transmission]

    # Price Range Filter
    try:
        price_min = request.args.get('price_min', '').strip().replace(',', '')
        if price_min:
            price_min = int(price_min)
            if price_min > 0:
                cars = [c for c in cars if c['price'] >= price_min]
    except:
        pass
    try:
        price_max = request.args.get('price_max', '').strip().replace(',', '')
        if price_max:
            price_max = int(price_max)
            if price_max > 0:
                cars = [c for c in cars if c['price'] <= price_max]
    except:
        pass

    # Sort Logic - default to price low to high
    sort_by = request.args.get('sort', 'price_low')
    if sort_by == 'alphabetical':
        cars.sort(key=lambda x: (x['brand'].lower(), x['model'].lower()))
    elif sort_by == 'reverse_alphabetical':
        cars.sort(key=lambda x: (x['brand'].lower(), x['model'].lower()), reverse=True)
    elif sort_by == 'price_low':
        cars.sort(key=lambda x: x['price'])
    elif sort_by == 'price_high':
        cars.sort(key=lambda x: x['price'], reverse=True)
    elif sort_by == 'newest':
        cars.sort(key=lambda x: x['year'], reverse=True)
    elif sort_by == 'oldest':
        cars.sort(key=lambda x: x['year'])

    # Pagination
    page = request.args.get('page', 1, type=int)
    per_page = 15
    total = len(cars)
    total_pages = math.ceil(total / per_page)
    start = (page - 1) * per_page
    end = start + per_page
    paginated_cars = cars[start:end]

    # Get unique brands for autocomplete
    all_brands = sorted(set(c['brand'] for c in get_approved_cars()))

    # No results - get recommended cars
    recommended_cars = []
    if total == 0:
        random.shuffle(cars) if cars else None
        all_cars = get_approved_cars()
        random.shuffle(all_cars)
        recommended_cars = all_cars[:6]

    return render_template('browse.html',
                           cars=paginated_cars,
                           page=page,
                           total_pages=total_pages,
                           total=total,
                           sort_by=sort_by,
                           body_types=BODY_TYPES,
                           fuel_types=FUEL_TYPES,
                           transmissions=TRANSMISSIONS,
                           all_brands=all_brands,
                           recommended_cars=recommended_cars,
                           trending_cars=get_trending_cars(5))


@app.route('/search')
def search():
    return redirect('/browse')

@app.route('/api/models/<brand>')
def get_models(brand):
    cars = get_approved_cars()
    models = sorted(set(c['model'] for c in cars if c['brand'].lower() == brand.lower()))
    return {"models": models}


@app.route('/photos/<path:folder>/<filename>')
def serve_car_photo(folder, filename):
    return send_from_directory(os.path.join(SOURCE_DIR, folder), filename)


@app.route('/car/<path:slug>')
def car_details(slug):
    folder_name = find_folder_by_slug(slug)
    if not folder_name:
        return redirect('/')

    db = get_db()
    listing = db.execute("""
        SELECT listings.*, users.username, users.phone as owner_phone
        FROM listings
        LEFT JOIN users ON listings.user_id = users.id
        WHERE listings.folder_name = ?
    """, (folder_name,)).fetchone()
    db.close()

    if not listing or listing['status'] != 'approved':
        return redirect('/')

    # Track this view
    if listing and listing['id']:
        track_car_view(listing['id'])

    folder_path = os.path.join(SOURCE_DIR, folder_name)

    words = folder_name.split()
    car_type = words[0] if words else ""
    year = next((w for w in words if w.isdigit() and len(w) == 4), "")
    
    # Get brand and model from listing (database)
    brand = listing['brand'] if listing and 'brand' in listing.keys() else ""
    model = listing['model'] if listing and 'model' in listing.keys() else ""
    
    # Construct display name from brand + model
    if brand and model:
        display_name = f"{brand} {model}"
    else:
        # Fallback to extracting from folder_name
        name_parts = []
        for w in words[1:]:
            if w.isdigit() and len(w) == 4: break
            name_parts.append(w)
        display_name = " ".join(name_parts)

    price_word = words[-1].lower() if words else ""
    try:
        clean_p = price_word.replace('m', '').replace('k', '')
        raw_price = float(clean_p)
        if 'm' in price_word:
            price_val = raw_price * 1000000
            display_price = f"₱{int(price_val):,}"
        elif 'k' in price_word:
            price_val = raw_price * 1000
            display_price = f"₱{int(price_val):,}"
        else:
            display_price = "Contact for Price"
    except:
        display_price = "Contact for Price"

    images = sorted([f for f in os.listdir(folder_path) if f.lower().endswith(('.jpg', '.png', '.jpeg'))])

    description = ""
    details_file = os.path.join(folder_path, "details.txt")
    if os.path.exists(details_file):
        with open(details_file, 'r', encoding="utf-8", errors="ignore") as f:
            description = f.read()

    return render_template('details.html',
                           name=display_name,
                           brand=brand,
                           model=model,
                           year=year,
                           car_type=car_type,
                           images=images,
                           description=description,
                           folder=folder_name,
                           display_price=display_price,
                           body_type=listing['body_type'] if 'body_type' in listing.keys() else None,
                           fuel_type=listing['fuel_type'] if 'fuel_type' in listing.keys() else None,
                           transmission=listing['transmission'] if 'transmission' in listing.keys() else None,
                           listing=listing,
                           original_price=listing['original_price'] if 'original_price' in listing.keys() else None)


@app.route('/register', methods=['GET', 'POST'])
def register():
    if 'user_id' in session:
        return redirect('/')
    
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        phone = request.form.get('phone', '').strip()
        password = request.form.get('password', '').strip()
        confirm_password = request.form.get('confirm_password', '').strip()

        if not all([username, phone, password]):
            flash('All fields are required.', 'danger')
            return redirect('/register')
        
        if len(username) < 3:
            flash('Username must be at least 3 characters.', 'danger')
            return redirect('/register')
        
        if len(password) < 6:
            flash('Password must be at least 6 characters.', 'danger')
            return redirect('/register')
        
        if password != confirm_password:
            flash('Passwords do not match.', 'danger')
            return redirect('/register')

        db = get_db()
        existing = db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        if existing:
            db.close()
            flash('Username already taken. Choose another.', 'danger')
            return redirect('/register')

        password_hash = generate_password_hash(password)
        db.execute("INSERT INTO users (username, phone, password_hash) VALUES (?, ?, ?)",
                   (username, phone, password_hash))
        db.commit()
        db.close()

        flash('Registration successful! Please log in.', 'success')
        return redirect('/login')

    return render_template('register.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'user_id' in session:
        return redirect('/')
    
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()

        if not username or not password:
            flash('Username and password are required.', 'danger')
            return redirect('/login')

        db = get_db()
        user = db.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        db.close()

        if user and check_password_hash(user['password_hash'], password):
            session['user_id'] = user['id']
            session['username'] = user['username']
            session['is_admin'] = bool(user['is_admin'])
            flash(f'Welcome back, {user["username"]}!', 'success')
            
            next_url = request.args.get('next')
            if next_url:
                return redirect(next_url)
            return redirect('/')
        else:
            flash('Invalid username or password.', 'danger')

    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect('/')


@app.route('/sell-car', methods=['GET', 'POST'])
@require_login
def sell_car():
    if request.method == 'POST':
        brand = request.form.get('brand', '').strip()
        model = request.form.get('model', '').strip()
        year = request.form.get('year', '').strip()
        price = request.form.get('price', '').strip().replace(',', '')
        car_type = request.form.get('car_type', '').strip()
        body_type = request.form.get('body_type', '').strip()
        fuel_type = request.form.get('fuel_type', '').strip()
        transmission = request.form.get('transmission', '').strip()
        contact = request.form.get('contact', '').strip()
        location = request.form.get('location', '').strip()
        description = request.form.get('description', '').strip()

        if not all([brand, model, year, price, car_type, contact, location]):
            flash('Please fill in all required fields.', 'danger')
            return redirect('/sell-car')

        try:
            price_int = int(price)
            if price_int <= 0:
                flash('Price must be greater than zero.', 'danger')
                return redirect('/sell-car')
        except ValueError:
            flash('Invalid price. Enter a number.', 'danger')
            return redirect('/sell-car')

        photos = request.files.getlist('photos')
        valid_photos = [f for f in photos if f.filename and allowed_file(f.filename)]
        if not valid_photos:
            flash('Please upload at least one photo (JPG or PNG).', 'danger')
            return redirect('/sell-car')

        folder_name = f"{car_type} {brand} {model} {year} {format_price_for_folder(price_int)}"
        folder_path = os.path.join(SOURCE_DIR, folder_name)

        counter = 1
        while os.path.exists(folder_path):
            folder_name = f"{car_type} {brand} {model} {year} {format_price_for_folder(price_int)} ({counter})"
            folder_path = os.path.join(SOURCE_DIR, folder_name)
            counter += 1

        os.makedirs(folder_path, exist_ok=True)

        photo_filenames = []
        for i, photo in enumerate(valid_photos):
            ext = photo.filename.rsplit('.', 1)[1].lower()
            filename = f"photo_{i+1}.{ext}"
            photo.save(os.path.join(folder_path, filename))
            photo_filenames.append(filename)

        with open(os.path.join(folder_path, "details.txt"), 'w', encoding='utf-8') as f:
            f.write(description if description else "")

        db = get_db()
        cursor = db.execute(
            """INSERT INTO listings
               (user_id, brand, model, year, price, original_price, car_type, body_type, fuel_type, transmission, owner_phone, location, description, photo_paths, folder_name, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (session['user_id'], brand, model, int(year), price_int, price_int, car_type, body_type or None, fuel_type or None, transmission or None, contact, location, description,
             ','.join(photo_filenames), folder_name, 'pending')
        )
        listing_id = cursor.lastrowid
        db.commit()
        db.close()

        db = get_db()
        user = db.execute("SELECT username FROM users WHERE id = ?", (session['user_id'],)).fetchone()
        db.close()
        username = user['username'] if user else 'Unknown'

        send_email_notification(brand, model, year, price_int, username, contact, listing_id)

        flash(f'Your {year} {brand} {model} has been submitted and is pending review!', 'success')
        return redirect('/my-cars')

    return render_template('sell_car.html', brands=CAR_BRANDS, body_types=BODY_TYPES, fuel_types=FUEL_TYPES, transmissions=TRANSMISSIONS)


@app.route('/my-cars')
@require_login
def my_cars():
    db = get_db()
    listings = db.execute(
        "SELECT * FROM listings WHERE user_id = ? ORDER BY created_at DESC",
        (session['user_id'],)
    ).fetchall()
    db.close()
    return render_template('my_cars.html', listings=listings)


@app.route('/my-cars/edit/<int:listing_id>', methods=['GET', 'POST'])
@require_login
def edit_listing(listing_id):
    db = get_db()
    row = db.execute("SELECT * FROM listings WHERE id = ? AND user_id = ?", (listing_id, session['user_id'])).fetchone()
    db.close()

    if not row:
        flash('Listing not found.', 'danger')
        return redirect('/my-cars')

    # Convert to dict to allow modification
    listing = dict(row)

    # Load description from details.txt if not in database
    if not listing.get('description'):
        folder_path = os.path.join(SOURCE_DIR, listing['folder_name'])
        details_file = os.path.join(folder_path, "details.txt")
        if os.path.exists(details_file):
            with open(details_file, 'r', encoding="utf-8", errors="ignore") as f:
                listing['description'] = f.read()

    if request.method == 'POST':
        brand = request.form.get('brand', '').strip()
        model = request.form.get('model', '').strip()
        year = request.form.get('year', '').strip()
        price = request.form.get('price', '').strip().replace(',', '')
        car_type = request.form.get('car_type', '').strip()
        body_type = request.form.get('body_type', '').strip()
        fuel_type = request.form.get('fuel_type', '').strip()
        transmission = request.form.get('transmission', '').strip()
        contact = request.form.get('contact', '').strip()
        location = request.form.get('location', '').strip()
        description = request.form.get('description', '').strip()

        if not all([brand, model, year, price, car_type, contact, location]):
            flash('Please fill in all required fields.', 'danger')
            return redirect(f'/my-cars/edit/{listing_id}')

        try:
            price_int = int(price)
            if price_int <= 0:
                flash('Price must be greater than zero.', 'danger')
                return redirect(f'/my-cars/edit/{listing_id}')
        except ValueError:
            flash('Invalid price.', 'danger')
            return redirect(f'/my-cars/edit/{listing_id}')

        old_folder = listing['folder_name']
        new_folder = f"{car_type} {brand} {model} {year} {format_price_for_folder(price_int)}"
        folder_path = os.path.join(SOURCE_DIR, new_folder)

        counter = 1
        while os.path.exists(folder_path) and new_folder != old_folder:
            new_folder = f"{car_type} {brand} {model} {year} {format_price_for_folder(price_int)} ({counter})"
            folder_path = os.path.join(SOURCE_DIR, new_folder)
            counter += 1

        if old_folder and new_folder != old_folder:
            old_path = os.path.join(SOURCE_DIR, old_folder)
            if os.path.exists(old_path):
                os.rename(old_path, folder_path)

            photos = request.files.getlist('photos')
            valid_photos = [f for f in photos if f.filename and allowed_file(f.filename)]
            if valid_photos:
                for i, photo in enumerate(valid_photos):
                    ext = photo.filename.rsplit('.', 1)[1].lower()
                    photo.save(os.path.join(folder_path, f"photo_{i+1}.{ext}"))
        else:
            photos = request.files.getlist('photos')
            valid_photos = [f for f in photos if f.filename and allowed_file(f.filename)]
            if valid_photos:
                existing_count = len([f for f in os.listdir(folder_path) if f.lower().endswith(('.jpg', '.png', '.jpeg'))])
                for i, photo in enumerate(valid_photos):
                    ext = photo.filename.rsplit('.', 1)[1].lower()
                    photo.save(os.path.join(folder_path, f"photo_{existing_count + i + 1}.{ext}"))

        with open(os.path.join(folder_path, "details.txt"), 'w', encoding='utf-8') as f:
            f.write(description if description else "")

        db = get_db()
        db.execute(
            """UPDATE listings
               SET brand=?, model=?, year=?, price=?, car_type=?, body_type=?, fuel_type=?, transmission=?, owner_phone=?, location=?, description=?, folder_name=?, status='pending', updated_at=CURRENT_TIMESTAMP
               WHERE id=? AND user_id=?""",
            (brand, model, int(year), price_int, car_type, body_type or None, fuel_type or None, transmission or None, contact, location, description, new_folder, listing_id, session['user_id'])
        )
        db.commit()
        db.close()

        flash('Listing updated and submitted for review.', 'success')
        return redirect('/my-cars')

    return render_template('edit_listing.html', listing=listing, brands=CAR_BRANDS, body_types=BODY_TYPES, fuel_types=FUEL_TYPES, transmissions=TRANSMISSIONS, is_admin=False)


@app.route('/my-cars/delete/<int:listing_id>', methods=['POST'])
@require_login
def delete_listing(listing_id):
    db = get_db()
    listing = db.execute("SELECT * FROM listings WHERE id = ? AND user_id = ?", (listing_id, session['user_id'])).fetchone()
    db.close()
    
    if not listing:
        flash('Listing not found.', 'danger')
        return redirect('/my-cars')

    if listing['folder_name']:
        folder_path = os.path.join(SOURCE_DIR, listing['folder_name'])
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)

    db = get_db()
    db.execute("DELETE FROM listings WHERE id = ? AND user_id = ?", (listing_id, session['user_id']))
    db.commit()
    db.close()

    flash('Listing deleted.', 'success')
    return redirect('/my-cars')


@app.route('/admin')
@require_admin
def admin_dashboard():
    db = get_db()
    status_filter = request.args.get('status', 'all')

    total_users = db.execute("SELECT COUNT(*) as count FROM users").fetchone()['count']

    if status_filter == 'pending':
        listings = db.execute("""
            SELECT listings.*, users.username, users.phone as owner_phone FROM listings
            JOIN users ON listings.user_id = users.id
            WHERE listings.status = 'pending' ORDER BY listings.created_at DESC
        """).fetchall()
    elif status_filter == 'approved':
        listings = db.execute("""
            SELECT listings.*, users.username, users.phone as owner_phone FROM listings
            JOIN users ON listings.user_id = users.id
            WHERE listings.status = 'approved' ORDER BY listings.created_at DESC
        """).fetchall()
    elif status_filter == 'rejected':
        listings = db.execute("""
            SELECT listings.*, users.username, users.phone as owner_phone FROM listings
            JOIN users ON listings.user_id = users.id
            WHERE listings.status = 'rejected' ORDER BY listings.created_at DESC
        """).fetchall()
    else:
        listings = db.execute("""
            SELECT listings.*, users.username, users.phone as owner_phone FROM listings
            JOIN users ON listings.user_id = users.id
            ORDER BY listings.created_at DESC
        """).fetchall()
    db.close()

    return render_template('admin.html', listings=listings, status_filter=status_filter, total_users=total_users)


@app.route('/admin/listing/<int:listing_id>/approve', methods=['POST'])
@require_admin
def approve_listing(listing_id):
    db = get_db()
    db.execute("UPDATE listings SET status = 'approved', updated_at = CURRENT_TIMESTAMP WHERE id = ?", (listing_id,))
    db.commit()
    db.close()
    flash('Listing approved and is now live.', 'success')
    return redirect('/admin')


@app.route('/admin/listing/<int:listing_id>/reject', methods=['POST'])
@require_admin
def reject_listing(listing_id):
    db = get_db()
    db.execute("UPDATE listings SET status = 'rejected', updated_at = CURRENT_TIMESTAMP WHERE id = ?", (listing_id,))
    db.commit()
    db.close()
    flash('Listing rejected.', 'warning')
    return redirect('/admin')


@app.route('/admin/listing/<int:listing_id>/delete', methods=['POST'])
@require_admin
def admin_delete_listing(listing_id):
    db = get_db()
    listing = db.execute("SELECT * FROM listings WHERE id = ?", (listing_id,)).fetchone()
    db.close()
    
    if not listing:
        flash('Listing not found.', 'danger')
        return redirect('/admin')

    if listing['folder_name']:
        folder_path = os.path.join(SOURCE_DIR, listing['folder_name'])
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)

    db = get_db()
    db.execute("DELETE FROM listings WHERE id = ?", (listing_id,))
    db.commit()
    db.close()

    flash('Listing deleted.', 'success')
    return redirect('/admin')


@app.route('/admin/listing/<int:listing_id>/edit', methods=['GET', 'POST'])
@require_admin
def admin_edit_listing(listing_id):
    db = get_db()
    row = db.execute("SELECT * FROM listings WHERE id = ?", (listing_id,)).fetchone()
    db.close()

    if not row:
        flash('Listing not found.', 'danger')
        return redirect('/admin')

    # Convert to dict to allow modification
    listing = dict(row)

    # Load description from details.txt if not in database
    if not listing.get('description'):
        folder_path = os.path.join(SOURCE_DIR, listing['folder_name'])
        details_file = os.path.join(folder_path, "details.txt")
        if os.path.exists(details_file):
            with open(details_file, 'r', encoding="utf-8", errors="ignore") as f:
                listing['description'] = f.read()

    if request.method == 'POST':
        brand = request.form.get('brand', '').strip()
        model = request.form.get('model', '').strip()
        year = request.form.get('year', '').strip()
        price = request.form.get('price', '').strip().replace(',', '')
        car_type = request.form.get('car_type', '').strip()
        body_type = request.form.get('body_type', '').strip()
        fuel_type = request.form.get('fuel_type', '').strip()
        transmission = request.form.get('transmission', '').strip()
        contact = request.form.get('contact', '').strip()
        location = request.form.get('location', '').strip()
        description = request.form.get('description', '').strip()
        new_status = request.form.get('status', listing['status'])

        if not all([brand, model, year, price, car_type, contact, location]):
            flash('Please fill in all required fields.', 'danger')
            return redirect(f'/admin/listing/{listing_id}/edit')

        try:
            price_int = int(price)
            if price_int <= 0:
                flash('Price must be greater than zero.', 'danger')
                return redirect(f'/admin/listing/{listing_id}/edit')
        except ValueError:
            flash('Invalid price.', 'danger')
            return redirect(f'/admin/listing/{listing_id}/edit')

        old_folder = listing['folder_name']
        new_folder = f"{car_type} {brand} {model} {year} {format_price_for_folder(price_int)}"
        folder_path = os.path.join(SOURCE_DIR, new_folder)

        counter = 1
        while os.path.exists(folder_path) and new_folder != old_folder:
            new_folder = f"{car_type} {brand} {model} {year} {format_price_for_folder(price_int)} ({counter})"
            folder_path = os.path.join(SOURCE_DIR, new_folder)
            counter += 1

        if old_folder and new_folder != old_folder:
            old_path = os.path.join(SOURCE_DIR, old_folder)
            if os.path.exists(old_path):
                os.rename(old_path, folder_path)
        elif not old_folder:
            os.makedirs(folder_path, exist_ok=True)

        photos = request.files.getlist('photos')
        valid_photos = [f for f in photos if f.filename and allowed_file(f.filename)]
        if valid_photos:
            if os.path.exists(folder_path):
                existing_count = len([f for f in os.listdir(folder_path) if f.lower().endswith(('.jpg', '.png', '.jpeg'))])
            else:
                existing_count = 0
                os.makedirs(folder_path, exist_ok=True)
            for i, photo in enumerate(valid_photos):
                ext = photo.filename.rsplit('.', 1)[1].lower()
                photo.save(os.path.join(folder_path, f"photo_{existing_count + i + 1}.{ext}"))

        if os.path.exists(folder_path):
            with open(os.path.join(folder_path, "details.txt"), 'w', encoding='utf-8') as f:
                f.write(description if description else "")

        db = get_db()
        db.execute(
            """UPDATE listings
               SET brand=?, model=?, year=?, price=?, car_type=?, body_type=?, fuel_type=?, transmission=?, owner_phone=?, location=?, description=?, folder_name=?, status=?, updated_at=CURRENT_TIMESTAMP
               WHERE id=?""",
            (brand, model, int(year), price_int, car_type, body_type or None, fuel_type or None, transmission or None, contact, location, description, new_folder, new_status, listing_id)
        )
        db.commit()
        db.close()

        flash('Listing updated successfully.', 'success')
        return redirect('/admin')

    return render_template('edit_listing.html', listing=listing, brands=CAR_BRANDS, body_types=BODY_TYPES, fuel_types=FUEL_TYPES, transmissions=TRANSMISSIONS, is_admin=True)


@app.route('/about')
def about():
    return render_template('about.html')


@app.route('/admin/users')
@require_admin
def admin_users():
    db = get_db()

    # Stats for dashboard cards
    total_users = db.execute("SELECT COUNT(*) as count FROM users").fetchone()['count']

    # Get listing counts for stat cards
    all_listings = db.execute("""
        SELECT listings.*, users.username, users.phone as owner_phone FROM listings
        JOIN users ON listings.user_id = users.id
        ORDER BY listings.created_at DESC
    """).fetchall()

    # User list with car counts
    users = db.execute("""
        SELECT users.*,
               (SELECT COUNT(*) FROM listings WHERE listings.user_id = users.id) as car_count
        FROM users ORDER BY created_at DESC
    """).fetchall()
    db.close()

    return render_template('admin_users.html', users=users, listings=all_listings, total_users=total_users)


@app.route('/admin/user/<int:user_id>')
@require_admin
def admin_user_detail(user_id):
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    raw_listings = db.execute("SELECT * FROM listings WHERE user_id = ? ORDER BY created_at DESC", (user_id,)).fetchall()
    db.close()
    if not user:
        flash('User not found.', 'danger')
        return redirect('/admin/users')

    # Add slug to each listing for clickable links
    listings = []
    for row in raw_listings:
        listing = dict(row)
        if listing.get('folder_name'):
            listing['slug'] = slugify(listing['folder_name'])
        else:
            listing['slug'] = f"listing-{listing['id']}"
        listings.append(listing)

    return render_template('admin_user_detail.html', user=user, listings=listings)


@app.route('/admin/user/<int:user_id>/delete', methods=['POST'])
@require_admin
def admin_delete_user(user_id):
    if session.get('user_id') == user_id:
        flash('You cannot delete your own account.', 'danger')
        return redirect('/admin/users')
    
    db = get_db()
    # Delete user's listings first
    db.execute("DELETE FROM listings WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    db.commit()
    db.close()
    
    flash('User and their listings have been deleted.', 'success')
    return redirect('/admin/users')


@app.route('/robots.txt')
def robots_txt():
    robots = """User-agent: *
Disallow: /login
Disallow: /register
Disallow: /sell-car
Disallow: /my-cars
Disallow: /admin
Disallow: /admin/
Disallow: /api/

Sitemap: https://www.usedcarsphilippines.com.ph/sitemap.xml"""
    return robots, 200, {'Content-Type': 'text/plain'}


@app.route('/sitemap.xml')
def sitemap():
    db = get_db()
    cars = db.execute("SELECT * FROM listings WHERE status = 'approved' ORDER BY created_at DESC").fetchall()
    db.close()
    
    base = "https://www.usedcarsphilippines.com.ph"
    
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    
    # Static pages (exclude /sell-car, /login, /register, /my-cars, /admin)
    static_pages = [('/', 'daily', '1.0'), ('/browse', 'daily', '0.9'), ('/about', 'monthly', '0.5')]
    for page, freq, prio in static_pages:
        xml += '  <url>\n'
        xml += f'    <loc>{base}{page}</loc>\n'
        xml += f'    <changefreq>{freq}</changefreq>\n'
        xml += f'    <priority>{prio}</priority>\n'
        xml += '  </url>\n'
    
    # Car listings (only approved)
    for car in cars:
        if car['folder_name']:
            slug = slugify(car['folder_name'])
            xml += '  <url>\n'
            xml += f'    <loc>{base}/car/{slug}</loc>\n'
            xml += f'    <lastmod>{str(car["updated_at"]).split()[0] if "updated_at" in car.keys() and car["updated_at"] else ""}</lastmod>\n'
            xml += '    <changefreq>weekly</changefreq>\n'
            xml += '    <priority>0.7</priority>\n'
            xml += '  </url>\n'
    
    xml += '</urlset>'
    return xml, 200, {'Content-Type': 'application/xml'}


if __name__ == '__main__':
    app.run(debug=True)
