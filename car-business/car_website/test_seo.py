from app import app
import sqlite3

print("=== SEO Fix Verification ===\n")

# Test 1: Robots.txt
print("1. Robots.txt:")
with app.test_client() as client:
    resp = client.get('/robots.txt')
    content = resp.data.decode('utf-8')
    if 'Disallow: /login' in content:
        print("   [OK] Blocks /login")
    else:
        print("   [FAIL] Does not block /login")
    if 'Disallow: /sell-car' in content:
        print("   [OK] Blocks /sell-car")
    else:
        print("   [FAIL] Does not block /sell-car")
    if 'Sitemap:' in content:
        print("   [OK] Includes Sitemap")
    else:
        print("   [FAIL] Missing Sitemap")

# Test 2: Sitemap.xml
print("\n2. Sitemap.xml:")
with app.test_client() as client:
    resp = client.get('/sitemap.xml')
    content = resp.data.decode('utf-8')
    if '/sell-car' not in content and '/login' not in content:
        print("   [OK] Excludes non-public pages")
    else:
        print("   [FAIL] Includes non-public pages")
    if 'https://www.usedcarsphilippines.com.ph' in content:
        print("   [OK] Uses https://www")
    else:
        print("   [FAIL] Does not use https://www")

# Test 3: HTTPS Redirect
print("\n3. HTTPS Redirect:")
with app.test_client() as client:
    resp = client.get('/', base_url='http://usedcarsphilippines.com.ph')
    if resp.status_code == 301:
        print("   [OK] HTTP -> HTTPS redirect works")
    else:
        print(f"   [FAIL] Status: {resp.status_code}")

# Test 4: WWW Redirect
print("\n4. WWW Redirect:")
with app.test_client() as client:
    resp = client.get('/', base_url='https://usedcarsphilippines.com.ph')
    if resp.status_code == 301:
        print("   [OK] Non-www -> www redirect works")
    else:
        print(f"   [FAIL] Status: {resp.status_code}")

print("\n=== All checks complete ===")
