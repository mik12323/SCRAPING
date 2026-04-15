import os
import time
import datetime
import re
import html
import json
from io import BytesIO
from truthbrush import Api
from discord_bot import DiscordNotifier
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

token = os.getenv("TRUTHSOCIAL_TOKEN")
api = Api(token=token)
webhook = os.getenv("TRUMP_HOOK")
gemini_key = os.getenv("GEMINI_KEY")

handle = "realDonaldTrump"
ID_FILE = "last_id.txt"

KEYWORDS = ['iran', 'war', 'ceasefire', 'israel', 'oil', 'gas']


def analyze_sentiment(text):
    if not gemini_key:
        return "UNKNOWN"
    
    try:
        client = genai.Client(api_key=gemini_key)
        
        prompt = f"""Analyze this news/truth and determine if it's BULLISH or BEARISH for cryptocurrency/gold/oil markets.

Rules:
- Output ONLY "BULLISH" or "BEARISH" (nothing else)
- Be concise and decisive
- Focus on crypto, gold, oil, economy impact

Content:
{text}"""

        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=prompt
        )
        
        result = response.text.strip().upper()
        if "BULLISH" in result:
            return "BULLISH"
        elif "BEARISH" in result:
            return "BEARISH"
        return "UNKNOWN"
        
    except Exception as e:
        print(f"AI analysis error: {e}")
        return "UNKNOWN"


if os.path.exists(ID_FILE):
    with open(ID_FILE, "r") as f:
        last_post_id = f.read().strip()
    print(f"--- Loaded Previous ID: {last_post_id} ---", flush=True)
else:
    last_post_id = ""
    print(f"--- Starting Fresh Monitor for @{handle} ---", flush=True)

while True:
    try:
        posts_gen = api.pull_statuses(handle)
        latest_post = next(posts_gen, None)
        
        if latest_post and isinstance(latest_post, dict):
            current_id = latest_post.get('id')
            
            if current_id != last_post_id:
                if last_post_id == "":
                    print(f"First run detected. Saving ID {current_id}", flush=True)
                    with open(ID_FILE, "w") as f:
                        f.write(current_id)
                    last_post_id = current_id
                    continue

                print(f"\nNEW POST DETECTED!", flush=True)
                
                is_retruth = latest_post.get('reblog')
                target_data = is_retruth if is_retruth else latest_post

                raw_content = target_data.get('content', '')
                
                text_with_newlines = re.sub(r'(<br\s*/?>|</p>)', '\n', raw_content)
                clean_text = re.sub(r'<[^>]+>', '', text_with_newlines)
                clean_text = html.unescape(clean_text).strip()
                
                found_keywords = [word for word in KEYWORDS if word in clean_text.lower()]
                
                if not found_keywords:
                    print("No relevant keywords, skipping", flush=True)
                    with open(ID_FILE, "w") as f:
                        f.write(current_id)
                    last_post_id = current_id
                    continue

                image_url = None
                attachments = target_data.get('media_attachments', [])
                if not attachments and is_retruth:
                    attachments = latest_post.get('media_attachments', [])

                if attachments:
                    image_url = attachments[0].get('url') or attachments[0].get('preview_url')

                if not clean_text or clean_text.strip() == "":
                    if is_retruth:
                        clean_text = f"RE-TRUTH from @{is_retruth['account']['username']}"
                    elif image_url:
                        clean_text = "[Photo/Video Post]"
                    else:
                        clean_text = "[Empty Post]"

                post_url = target_data.get('url', latest_post.get('url'))
                
                print(f"Analyzing sentiment...", flush=True)
                sentiment = analyze_sentiment(clean_text)
                print(f"Sentiment: {sentiment}", flush=True)
                
                color = 3447003 if sentiment == "BULLISH" else (13632027 if sentiment == "BEARISH" else 8421504)
                
                header = f"**{sentiment}**"
                
                DiscordNotifier.post(webhook, f"@{handle} - {sentiment} Signal", clean_text, post_url, image_url, header)

                with open(ID_FILE, "w") as f:
                    f.write(current_id)
                last_post_id = current_id
                print(f"Alert Sent for ID: {current_id}", flush=True)

    except Exception as e:
        print(f"Error: {e}", flush=True)
        time.sleep(10)
    time.sleep(60)