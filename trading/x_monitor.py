import os
import sys
import time
import json
import random
from io import BytesIO
from datetime import datetime

import requests
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

DISCORD_WEBHOOK = os.getenv("X_HOOK")

ACCOUNTS = ["DeItaone"]
KEYWORDS = ["🚨", "trump", "iran", "war", "ceasefire"]


def analyze_crypto_sentiment(tweet_text):
    my_key = os.getenv("GEMINI_KEY")
    if not my_key:
        print("No GEMINI_KEY found in .env")
        return "UNKNOWN"
    
    try:
        client = genai.Client(api_key=my_key)
        
        prompt = f"""Analyze this news/tweet and determine if it's BULLISH or BEARISH for cryptocurrency markets.

Rules:
- Output ONLY "BULLISH" or "BEARISH" or "NEUTRAL" (nothing else)
- Be concise and decisive
- Focus on crypto market impact

Tweet content:
{tweet_text}"""

        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=prompt
        )
        
        result = response.text.strip().upper()
        if "BULLISH" in result:
            return "BULLISH"
        elif "BEARISH" in result:
            return "BEARISH"
        else:
            return "NEUTRAL"
            
    except Exception as e:
        print(f"AI analysis error: {e}")
        return "NEUTRAL"


def check_for_keywords(text, keywords):
    text_lower = text.lower()
    return [kw for kw in keywords if kw.lower() in text_lower]


def send_alert(username, tweet_data, sentiment):
    if not DISCORD_WEBHOOK:
        print("No DISCORD_WEBHOOK configured!")
        return
    
    color = 3447003 if sentiment == "BULLISH" else (13632027 if sentiment == "BEARISH" else 8421504)
    
    embed = {
        "title": f"@{username} - {sentiment} Signal",
        "description": tweet_data.get('text', 'No content')[:3900],
        "url": tweet_data.get('url', ''),
        "footer": {"text": f"AI Crypto Sentiment Analysis"},
        "color": color,
        "fields": [
            {"name": "Sentiment", "value": sentiment, "inline": True},
            {"name": "Time", "value": tweet_data.get('time', 'Unknown'), "inline": True}
        ]
    }
    
    ping_content = f"@everyone **{sentiment}** 🚀"
    
    if tweet_data.get('media'):
        try:
            img_data = requests.get(tweet_data['media']).content
            files = {"file": ("tweet_image.jpg", BytesIO(img_data), "image/jpeg")}
            embed["image"] = {"url": "attachment://tweet_image.jpg"}
            
            payload = {"content": ping_content, "embeds": [embed]}
            response = requests.post(
                DISCORD_WEBHOOK,
                files=files,
                data={"payload_json": json.dumps(payload)}
            )
            if response.status_code not in [200, 204]:
                print(f"Discord error: {response.text}")
            return
        except Exception as e:
            print(f"Image download failed: {e}")
    
    payload = {"content": ping_content, "embeds": [embed]}
    response = requests.post(DISCORD_WEBHOOK, json=payload)
    if response.status_code not in [200, 204]:
        print(f"Discord error: {response.status_code}: {response.text}")


def get_last_id(username):
    path = f"last_id_{username}.txt"
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read().strip()
    return None


def save_last_id(username, tweet_id):
    with open(f"last_id_{username}.txt", "w") as f:
        f.write(str(tweet_id))


def human_delay(min_sec=1, max_sec=3):
    time.sleep(random.uniform(min_sec, max_sec))


def get_tweets(username, since_id=None):
    tweets = []
    
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp("http://localhost:9222")
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else context.new_page()
        
        print(f"  Navigating to @{username} profile...")
        page.goto(f"https://x.com/{username}", wait_until="domcontentloaded")
        human_delay(2, 4)
        
        page.wait_for_selector('article[data-testid="tweet"]', timeout=15000)
        
        page.evaluate("window.scrollBy(0, 2000)")
        human_delay(1, 2)
        
        page.evaluate("window.scrollBy(0, -2000)")
        human_delay(1, 2)
        
        page.wait_for_selector('article[data-testid="tweet"]', timeout=10000)
        articles = page.locator('article[data-testid="tweet"]').all()
        
        print(f"  Found {len(articles)} tweets on page")
        
        all_tweets_data = []
        
        for article in articles:
            try:
                link_elem = article.locator('a[href*="/status/"]').first
                href = link_elem.get_attribute('href')
                
                import re
                match = re.search(r'/status/(\d+)', href)
                tweet_id = match.group(1) if match else None
                
                if not tweet_id:
                    continue
                
                time_elem = article.locator('time').first
                datetime_val = time_elem.get_attribute('datetime') if time_elem.count() > 0 else None
                
                text_elem = article.locator('[data-testid="tweetText"]')
                tweet_text = text_elem.first.inner_text() if text_elem.count() > 0 else ""
                
                media_img = article.locator('img[src*="media"]').first
                media_url = media_img.get_attribute('src') if media_img.count() > 0 else None
                
                tweet_url = f"https://x.com{href}"
                
                all_tweets_data.append({
                    'id': tweet_id,
                    'text': tweet_text,
                    'url': tweet_url,
                    'time': datetime_val,
                    'media': media_url
                })
            except Exception as e:
                continue
        
        if all_tweets_data:
            all_tweets_data.sort(key=lambda x: x['time'] or '', reverse=True)
            latest_tweet = all_tweets_data[0]
            print(f"  Latest tweet datetime: {latest_tweet['time']}")
            tweets.append(latest_tweet)
    
    return tweets


def run_once():
    for username in ACCOUNTS:
        print(f"\nChecking @{username}...")
        since_id = get_last_id(username)
        
        try:
            tweets = get_tweets(username, since_id)
            
            if not tweets:
                print("  No tweets found")
                continue
            
            tweet = tweets[0]
            print(f"  Latest Tweet: {tweet['text'][:100]}...")
            
            if since_id and str(tweet['id']) == str(since_id):
                print("  Already seen, skipping")
                continue
            
            matched = check_for_keywords(tweet['text'], KEYWORDS)
            
            if matched:
                print(f"  Found match! Tweet ID: {tweet['id']}, keywords: {matched}")
                print("  Analyzing sentiment...")
                sentiment = analyze_crypto_sentiment(tweet['text'])
                print(f"  Sentiment: {sentiment}")
                send_alert(username, tweet, sentiment)
            else:
                print("  No keyword match")
            
            save_last_id(username, tweet['id'])
            print(f"  Updated last ID to {tweet['id']}")
            
        except Exception as e:
            print(f"Error fetching tweets for @{username}: {e}")
        
        time.sleep(2)


def run_loop(interval=60):
    print(f"Starting X Monitor (checking every {interval} seconds)...")
    print("Press Ctrl+C to stop.\n")
    
    while True:
        try:
            run_once()
            jitter = random.uniform(-10, 10)
            sleep_time = interval + jitter
            print(f"  Next check in {sleep_time:.0f} seconds...")
            time.sleep(sleep_time)
        except KeyboardInterrupt:
            print("\nStopped.")
            break


if __name__ == "__main__":
    if "--loop" in sys.argv:
        interval = 60
        if len(sys.argv) > 2:
            try:
                interval = int(sys.argv[2])
            except:
                pass
        run_loop(interval)
    else:
        run_once()
