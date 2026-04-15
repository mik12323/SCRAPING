import os
import sys
import time
import random

from dotenv import load_dotenv
import requests
from google import genai

load_dotenv()

DISCORD_WEBHOOK = os.getenv("TELEGRAM_HOOK", "").strip().strip("'").strip('"')
GEMINI_KEY = os.getenv("GEMINI_KEY", "").strip().strip("'").strip('"')

API_ID = os.getenv("TELEGRAM_API_ID", "").strip().strip("'").strip('"')
API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip().strip("'").strip('"')
SESSION_NAME = "telegram_monitor"

CHAT_NAMES = ["Alpha", "Walter Bloomberg", "World Geo Political News", "infinityhedge", "Watcher Guru", "Insider Paper", "Tree News", "Solid Intel 🛰", "Market News Feed"]

KEYWORDS = ['iran', 'war', 'ceasefire', 'israel', 'oil', 'gas', 'trump']

processed_ids = set()


def analyze_sentiment(text):
    if not GEMINI_KEY:
        return "UNKNOWN"
    
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        
        prompt = f"""Analyze this news and determine if it's BULLISH or BEARISH for cryptocurrency market. Reason out 3 times before answering.

Output ONLY "BULLISH" or "BEARISH" OR "NEUTRAL" (nothing else)

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
        return "NEUTRAL"
        
    except Exception as e:
        print(f"AI analysis error: {e}")
        return "NEUTRAL"


def send_to_discord(title, text, url, sentiment):
    if not DISCORD_WEBHOOK:
        print("No DISCORD_WEBHOOK configured!")
        return
    
    color = 3447003 if sentiment == "BULLISH" else (13632027 if sentiment == "BEARISH" else 8421504)
    
    embed = {
        "title": title,
        "description": text[:3900] if text else "No content",
        "url": url,
        "color": color,
        "fields": [
            {"name": "Sentiment", "value": sentiment, "inline": True},
            {"name": "Source", "value": "Telegram", "inline": True}
        ]
    }
    
    ping_content = f"**{sentiment}**"
    
    payload = {"content": ping_content, "embeds": [embed]}
    response = requests.post(DISCORD_WEBHOOK, json=payload)
    if response.status_code not in [200, 204]:
        print(f"Discord error: {response.status_code}: {response.text}")


def check_keywords(text):
    if not text:
        return []
    text_lower = text.lower()
    return [kw for kw in KEYWORDS if kw in text_lower]


async def main():
    from telethon import TelegramClient
    from telethon.events import NewMessage
    
    print("Connecting to Telegram...")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    print("Connected!")
    
    print(f"Monitoring groups: {CHAT_NAMES}")
    
    target_entities = []
    dialogs = await client.get_dialogs()
    for dialog in dialogs:
        if dialog.name in CHAT_NAMES:
            target_entities.append(dialog.entity)
            print(f"  Found: {dialog.name}")
    
    if not target_entities:
        print("No matching groups found!")
        return
    
    print("\nListening for new messages in real-time...")
    print("Press Ctrl+C to stop\n")
    
    async def handle_new_message(event):
        chat = await event.get_chat()
        chat_name = chat.title if hasattr(chat, 'title') else str(chat)
        
        if chat_name not in CHAT_NAMES:
            return
        
        msg_id = f"{chat_name}_{event.message.id}"
        if msg_id in processed_ids:
            return
        
        processed_ids.add(msg_id)
        if len(processed_ids) > 1000:
            processed_ids.clear()
        
        if not event.message.text:
            return
        
        found_keywords = check_keywords(event.message.text)
        
        if not found_keywords:
            return
        
        print(f"\n[MATCH] {chat_name}")
        print(f"  Keywords: {found_keywords}")
        print(f"  Message: {event.message.text[:100]}...")
        
        sentiment = analyze_sentiment(event.message.text)
        print(f"  Sentiment: {sentiment}")
        
        title = f"Telegram - {chat_name}"
        url = None
        if hasattr(chat, 'username') and chat.username:
            url = f"https://t.me/{chat.username}/{event.message.id}"
        
        send_to_discord(title, event.message.text, url, sentiment)
        
        time.sleep(random.uniform(1, 3))
    
    client.add_event_handler(handle_new_message, NewMessage(incoming=True))
    
    await client.run_until_disconnected()


if __name__ == "__main__":
    if not API_ID or not API_HASH:
        print("Error: TELEGRAM_API_ID and TELEGRAM_API_HASH not set in .env")
        sys.exit(1)
    
    if not CHAT_NAMES:
        print("Please edit CHAT_NAMES in the code with your actual Telegram group names")
        sys.exit(1)
    
    import asyncio
    asyncio.run(main())
