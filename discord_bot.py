import requests
import io

class DiscordNotifier:
    @staticmethod
    def post(webhook_url, title, description, post_link, image_url=None, word=""):
        # 1. Clean up description length
        if description and len(description) > 3900:
            description = description[:3900] + "..."

        # 2. Build the Embed
        embed = {
            "title": title,
            "description": description,
            "url": post_link,
            "footer": {"text": "Sent by Sauce's Fed Monitor Bot"},
            "color": 65280 
        }

        files = {}
        # 3. THE FIX: Download the image so Discord doesn't have to fetch it
        if image_url and str(image_url).lower() != "none":
            try:
                # We use a 'Human' header so Truth Social doesn't block the download
                headers = {'User-Agent': 'Mozilla/5.0'}
                img_data = requests.get(image_url.strip(), headers=headers).content
                
                # Turn the raw data into a file Discord can read
                file_name = "trump_post.jpg"
                files = {"file": (file_name, io.BytesIO(img_data), "image/jpeg")}
                
                # Tell the embed to use this specific attached file
                embed["image"] = {"url": f"attachment://{file_name}"}
            except Exception as e:
                print(f"📸 Image Download Failed: {e}")

        # 4. Clean up the Keyword/Ping text
        if word and str(word).lower() != "none" and word.strip() != "":
            content = f"{word.strip()} mentioned @everyone"
        else:
            content = "@everyone"

        # 5. Send to Discord
        payload = {
            "content": content,
            "embeds": [embed]
        }

        try:
            # We use 'files' for the upload and 'payload_json' for the text
            import json
            response = requests.post(
                webhook_url, 
                files=files, 
                data={"payload_json": json.dumps(payload)}
            )
            if response.status_code not in [200, 204]:
                print(f"❌ Discord Error {response.status_code}: {response.text}")
        except Exception as e:
            print(f"⚠️ Failed to send to Discord: {e}")