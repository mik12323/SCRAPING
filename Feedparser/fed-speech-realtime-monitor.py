import feedparser
import time
import os
from dotenv import load_dotenv

# This searches for the .env file and loads the variables
load_dotenv()

# Now you can grab the keys using their names
my_webhook = os.getenv("FED_SPEECH_HOOK")
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from base_ai import Base_Ai
from discord_bot import DiscordNotifier

# 1. Setup our "Memory" file
CHECKPOINT_FILE = "latest_speech_date.txt"
RSS_URL = "https://www.federalreserve.gov/feeds/speeches.xml"
webhook_url = my_webhook
send_embed = DiscordNotifier(webhook_url)

def get_last_saved_date():
    # If the file exists, read the date. If not, return None.
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return f.read().strip()
    return None

def save_new_date(new_date):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(new_date)

print("Starting Real-Time Monitor... Press Ctrl+C to stop.")

while True:
    # 2. Grab the latest feed data
    feed = feedparser.parse(RSS_URL)
    
    if len(feed.entries) > 0:
        latest_post = feed.entries[0]
        latest_date = latest_post.published
        latest_link = latest_post.link
        last_saved = get_last_saved_date()

        # 3. COMPARE: Is the feed date different from our notepad?
        if latest_date != last_saved:
            print(f"\n*** NEW POST DETECTED! ***")
            print(f"Title: {latest_post.title}")
            print(f"Date: {latest_date}")
            print(f"Link: {latest_link}")
            
            # 4. UPDATE the notepad so we don't notify again for this post
            save_new_date(latest_date)
            print("Now asking Gemini for analysis")
            prompt = "I JUST WANT TO KNOW THEY KEY POINTS AND TELL ME IF ITS BULLISH base on the speech even if it is not mentioned in 4 sectors GOLD, Dollar, Crypto, Economy AND BEARISH AND TELL WHY USE SIMPLE ENGLISH DONT MAKE IT TOO LONG RITICAL: Keep your answer under 1500 characters so it fits in a message."
            answer = Base_Ai.bullish_or_bearish(latest_link, prompt)
            # print(f"AI Analysis: {answer}")
            send_embed.post(latest_post.title, answer, latest_link)
        else:
            print("No new posts. Checking again in 60 seconds...", end="\r")
        
        

    # 5. WAIT before checking again (don't spam the server!)
    time.sleep(60)