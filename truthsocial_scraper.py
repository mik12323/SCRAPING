import os
import time
import datetime
from truthbrush import Api
from discord_bot import DiscordNotifier
from base_ai import Base_Ai
from dotenv import load_dotenv

# 1. Setup
load_dotenv()
api = Api(username=os.getenv("TRUTHSOCIAL_USERNAME"), 
          password=os.getenv("TRUTHSOCIAL_PASSWORD"),
          )
webhook = os.getenv("TRUMP_HOOK")

handle = "realDonaldTrump"
ID_FILE = "last_id.txt"

# 2. Load Memory (Check if we already have a saved ID)
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
                # Is this the very first time the script is running?
                if last_post_id == "":
                    print(f"⚙️ First run detected. Saving ID {current_id} to memory. Not calling Gemini yet.", flush=True)
                    with open(ID_FILE, "w") as f:
                        f.write(current_id)
                    last_post_id = current_id

                # Is it an ACTUALLY new post?
                elif current_id != last_post_id:
                    print(f"🚨 NEW POST DETECTED! Calling Gemini...", flush=True)
                    
                    # 1. Extract Text & Image
                    content = latest_post.get('content', '').strip()
                    image_url = None
                    attachments = latest_post.get('media_attachments', [])
                    if attachments:
                        image_url = attachments[0].get('url')

                    clean_text = content.replace('<p>', '').replace('</p>', '').replace('<br/>', '\n')
                    if not clean_text and image_url:
                        clean_text = "[Photo Post]"

                    # 2. Ask Gemini
                    try:
                        prompt = "Tell me if this post/image is bullish or bearish on Economy, Gold, and Crypto."
                        analysis = Base_Ai.general_question(clean_text, prompt, image_url)
                    except Exception as e:
                        print(f"Gemini Error: {e}", flush=True)
                        analysis = "AI analysis currently unavailable (Rate Limit)."

                    # 3. Send to Discord
                    DiscordNotifier.post(webhook, "New Trump Activity!", analysis, latest_post['url'], image_url)

                    # 4. Save Memory
                    with open(ID_FILE, "w") as f:
                        f.write(current_id)
                    last_post_id = current_id

    except Exception as e:
        print(f"\n⚠️ Error: {e}", flush=True)
        time.sleep(10)

    # 4. Heartbeat Countdown (120s is safer to avoid blocks)
    for i in range(120, 0, -1):
        now = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"[{now}] Monitoring @{handle}... Next check in {i}s    ", end="\r", flush=True)
        time.sleep(1)